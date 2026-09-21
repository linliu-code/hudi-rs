/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! ENG-40156 — Substrait predicate pushdown from Velox into hudi-rs.
//!
//! Wire format: `substrait::proto::ExtendedExpression` serialized via prost.
//! Velox `HudiSplitReader` builds this from the Substrait filter that Gluten
//! originally produced (preserved verbatim across the SubstraitToVeloxPlan
//! conversion), then forwards the bytes through the cxx-rs bridge. We decode
//! the expression here and evaluate it against the post-merge arrow
//! RecordBatch using arrow kernels.
//!
//! ## Supported shapes
//!
//! - **Comparisons**: `equal`, `not_equal`, `lt`, `lte`, `gt`, `gte`
//! - **Null tests**: `is_null`, `is_not_null`
//! - **Boolean**: `and`, `or`, `not`
//! - **IN lists**: `SingularOrList` (`x IN (a, b, c)`), evaluated as the
//!   Kleene OR-fold of equality comparisons against each literal option
//! - **Operands**: column reference (top-level struct field), typed literal
//!   (bool / i32 / i64 / f32 / f64 / string / null)
//!
//! A pushed expression that *calls* anything outside this set is dropped at
//! decode time (the whole predicate is discarded and a warning is logged).
//! Unsupported functions merely *declared* in the plan's function table are
//! ignored — Gluten serialises the whole plan's table into the blob, so an
//! unrelated `sum` / `cast` / `substring` entry must not cost us pushdown for
//! a predicate that never references it. Velox's post-scan filter still
//! evaluates the original expression on every batch, so correctness is
//! preserved when we drop — we only lose the perf benefit of early filtering.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow::compute::kernels::arity::unary;
use arrow::compute::kernels::boolean;
use arrow::compute::kernels::cast::{CastOptions, cast_with_options};
use arrow::compute::kernels::cmp;
use arrow::error::ArrowError;
use arrow::util::display::FormatOptions;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Datum, Decimal128Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, RecordBatch, Scalar, StringArray, TimestampNanosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use arrow_select::filter::filter_record_batch;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowPredicate, RowFilter};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::schema::types::SchemaDescriptor;
use prost::Message;
use substrait::proto::{
    Expression, ExtendedExpression,
    expression::{
        FieldReference, Literal, RexType, ScalarFunction, SingularOrList, field_reference,
        literal::LiteralType, reference_segment,
    },
    expression_reference,
    extensions::simple_extension_declaration,
    function_argument,
};

// ════════════════════════════════════════════════════════════════════════════
// Public API
// ════════════════════════════════════════════════════════════════════════════

/// A decoded ExtendedExpression, ready to evaluate against arrow batches.
///
/// Constructed once from the substrait wire bytes when the file-group reader
/// is created; evaluated against each RecordBatch produced by the merge.
#[derive(Debug, Clone)]
pub struct PushedFilter {
    /// substrait function_anchor → known operator we know how to evaluate.
    function_map: HashMap<u32, KnownFunction>,
    /// Schema field index → column name. Substrait Selection refers to fields
    /// by index; hudi-rs RecordBatch columns are looked up by name.
    column_names: Vec<String>,
    /// The boolean expression to evaluate against each batch.
    expression: Expression,
}

impl PushedFilter {
    /// Decode an ExtendedExpression from prost-encoded bytes.
    ///
    /// Returns:
    /// - `Ok(None)` if bytes is empty (caller pushed no predicate), if the
    ///   pushed expression references a function we can't evaluate, or if the
    ///   wire shape is otherwise unsupported. In all these cases we fall back
    ///   to Velox's post-scan filter for correctness.
    /// - `Ok(Some(filter))` if every function **referenced by the pushed
    ///   expression** is one we know how to evaluate. Unknown declarations that
    ///   the expression never references are ignored: Gluten serialises the
    ///   whole plan's function table into this blob, so unrelated entries
    ///   (`sum`, `cast`, `substring`, …) must not cost us pushdown.
    /// - `Err(_)` only for hard decode errors (malformed protobuf bytes).
    pub fn decode(bytes: &[u8]) -> Result<Option<Self>, String> {
        if bytes.is_empty() {
            return Ok(None);
        }
        let ext_expr = ExtendedExpression::decode(bytes)
            .map_err(|e| format!("[ENG-40156] failed to decode ExtendedExpression: {e}"))?;
        let ExtendedExpression {
            base_schema,
            extensions,
            referred_expr,
            ..
        } = ext_expr;

        let column_names = match base_schema {
            Some(schema) => schema.names,
            None => {
                log::warn!(
                    "[ENG-40156] ExtendedExpression missing base_schema; \
                     dropping pushed filter, relying on Velox post-scan filter"
                );
                return Ok(None);
            }
        };

        let expression = match referred_expr.into_iter().next() {
            Some(re) => match re.expr_type {
                Some(expression_reference::ExprType::Expression(e)) => e,
                _ => {
                    log::warn!(
                        "[ENG-40156] ExtendedExpression.referred_expr[0] is not \
                         an Expression (likely a Measure); dropping pushed filter"
                    );
                    return Ok(None);
                }
            },
            None => {
                log::warn!(
                    "[ENG-40156] ExtendedExpression had no referred expression; \
                     dropping pushed filter, relying on Velox post-scan filter"
                );
                return Ok(None);
            }
        };

        // Only the anchors the pushed expression actually calls matter.
        let mut referenced_anchors = BTreeSet::new();
        Self::collect_function_anchors(&expression, &mut referenced_anchors);

        // Walk extensions table → build function anchor → KnownFunction map.
        // Declarations we don't recognise are skipped rather than fatal; the
        // referenced-anchor check below decides whether that costs us pushdown.
        let mut function_map = HashMap::new();
        let mut declared_names: HashMap<u32, &str> = HashMap::new();
        for decl in &extensions {
            if let Some(simple_extension_declaration::MappingType::ExtensionFunction(f)) =
                &decl.mapping_type
            {
                declared_names.insert(f.function_anchor, f.name.as_str());
                match KnownFunction::from_name(&f.name) {
                    Some(known) => {
                        function_map.insert(f.function_anchor, known);
                    }
                    None => log::debug!(
                        "[ENG-40156] declared substrait function '{}' (anchor={}) is not \
                         evaluable here; ignoring unless the pushed expression uses it",
                        f.name,
                        f.function_anchor
                    ),
                }
            }
        }

        for anchor in &referenced_anchors {
            if function_map.contains_key(anchor) {
                continue;
            }
            match declared_names.get(anchor) {
                Some(name) => log::warn!(
                    "[ENG-40156] pushed expression references unsupported substrait \
                     function '{name}' (anchor={anchor}); dropping pushed filter, \
                     relying on Velox post-scan filter"
                ),
                None => log::warn!(
                    "[ENG-40156] pushed expression references function anchor {anchor} \
                     with no declaration in the extensions table; dropping pushed \
                     filter, relying on Velox post-scan filter"
                ),
            }
            return Ok(None);
        }

        Ok(Some(Self {
            function_map,
            column_names,
            expression,
        }))
    }

    /// The whole base schema, in substrait field-index order — NOT the subset the
    /// expression references. Gluten serialises the scan's base schema into the
    /// blob, so a predicate on one column arrives with every column named here.
    /// Use [`Self::referenced_columns`] for anything scoped to the predicate.
    pub fn columns(&self) -> &[String] {
        &self.column_names
    }

    /// The columns the pushed expression actually references, deduped and in
    /// field-index order. Any decision about what the predicate can misread must
    /// key on this; [`Self::columns`] would widen it to the whole scan.
    ///
    /// An unresolvable field index is SKIPPED, which narrows the result — so a
    /// predicate whose column cannot be resolved does not arm the repair guard for
    /// that column. That would be the unsafe direction if anything downstream still
    /// pushed the predicate.
    ///
    /// There are THREE consumers of that decision, and each is covered differently:
    ///
    /// * the row filter — `build_row_filter` refuses the same unresolvable plan
    ///   outright, so no filter is installed and there is nothing for a disarmed
    ///   guard to misread. This is the compensating control, and NOT
    ///   `references_only_primary_keys`, which an earlier version of this comment
    ///   named: `base_read_pushdown_is_safe()` short-circuits to `true` on any
    ///   split with no log files, so the primary-key gate never runs on a CoW or
    ///   base-only slice, which is precisely where a disarmed guard would
    ///   over-drop.
    /// * the row-group selector — built directly from `pushed_filter`, NOT through
    ///   `build_row_filter`, so the control above does not reach it. It is safe
    ///   only incidentally: `comparison_can_match` never prunes a `Timestamp`
    ///   column, so a mislabelled one cannot cost row groups. That is a property
    ///   of the pruning code, not of this function, and it is worth naming because
    ///   it is the kind of thing a later optimisation quietly removes.
    /// * the injected provider — applies the CALLER's predicate, not ours, so
    ///   neither control above touches it. Covered instead in
    ///   `repair_risk_columns_for`: when the predicate is opaque to us the risk set
    ///   is taken from the table schema rather than left empty.
    pub fn referenced_columns(&self) -> Vec<String> {
        self.referenced_field_indices()
            .into_iter()
            .filter_map(|idx| self.column_names.get(idx).cloned())
            .collect()
    }

    /// Returns true iff every column this filter references is either in
    /// `pk_field_names` or is the `_hoodie_record_key` meta column.
    ///
    /// Mirrors Java's
    /// `SparkFileFormatInternalRowReaderContext.filterIsSafeForPrimaryKey`
    /// (`hudi-client/hudi-spark-client/.../SparkFileFormatInternalRowReaderContext.scala:285-289`):
    ///
    /// ```scala
    /// def filterIsSafeForPrimaryKey(filter: Filter, recordKeyFields: Set[String]): Boolean = {
    ///   filter.references.forall(c =>
    ///     recordKeyFields.contains(c.toLowerCase) ||
    ///     c.equalsIgnoreCase(HoodieRecord.RECORD_KEY_METADATA_FIELD))
    /// }
    /// ```
    ///
    /// Used to decide whether predicate pushdown is safe for **MERGE_ON_READ**
    /// readers (base file + parquet log blocks). Hudi requires primary keys to
    /// be immutable across upserts — a log update changes columns but never
    /// the PK — so a predicate over PK columns has the same outcome pre- and
    /// post-merge. Predicates over non-PK columns can be flipped by log
    /// updates and must wait for post-merge evaluation.
    ///
    /// Comparison is case-insensitive (matches Java's `.toLowerCase` /
    /// `.equalsIgnoreCase` semantics). The empty-reference case (e.g. a
    /// constant predicate) returns `true` — vacuously safe.
    ///
    /// Returns `false` defensively if any referenced substrait index is out
    /// of range of `column_names` (would indicate a malformed plan; treat as
    /// unsafe).
    pub fn references_only_primary_keys(&self, pk_field_names: &[String]) -> bool {
        let mut pk_set: std::collections::HashSet<String> =
            pk_field_names.iter().map(|s| s.to_lowercase()).collect();
        // Always allow the meta-column form. Java does this via the explicit
        // `equalsIgnoreCase(HoodieRecord.RECORD_KEY_METADATA_FIELD)` branch.
        pk_set.insert("_hoodie_record_key".to_string());

        for idx in self.referenced_field_indices() {
            let name = match self.column_names.get(idx) {
                Some(n) => n.to_lowercase(),
                None => return false,
            };
            if !pk_set.contains(&name) {
                return false;
            }
        }
        true
    }

    /// ENG-47483 — choose which row groups can be skipped outright, from the
    /// footer statistics parquet has already fetched.
    ///
    /// This is the only mechanism on the FFI path that avoids reading bytes. A
    /// `RowFilter` decides per row after the predicate columns are decoded, so
    /// it saves decode and never IO; a row group rejected here is never
    /// fetched.
    ///
    /// # Why this is conservative, and must stay that way
    ///
    /// Velox re-filters whatever we return, so KEEPING a row group that cannot
    /// match costs only time. DROPPING one that can match silently loses rows,
    /// and nothing downstream can restore them: the post-scan filter can only
    /// remove rows, never add them back. Every branch here therefore answers
    /// "could this group contain a matching row?" and defaults to `true`
    /// whenever the statistics do not *prove* otherwise: missing stats, an
    /// unknown function, an unresolvable column, or a negation.
    ///
    /// Returns `None` when nothing could be pruned, so the caller can skip
    /// calling `with_row_groups` at all rather than passing the full list.
    pub fn select_row_groups(&self, metadata: &ParquetMetaData) -> Option<Vec<usize>> {
        let groups = metadata.row_groups();
        let keep: Vec<usize> = (0..groups.len())
            .filter(|&i| self.group_can_match(&self.expression, &groups[i]))
            .collect();
        if keep.len() == groups.len() {
            return None;
        }
        Some(keep)
    }

    /// `true` if `rg` might contain a row satisfying `expr`. See
    /// [`Self::select_row_groups`] for why the default is `true`.
    #[cfg(test)]
    fn group_can_match_pub(&self, rg: &RowGroupMetaData) -> bool {
        self.group_can_match(&self.expression, rg)
    }

    fn group_can_match(&self, expr: &Expression, rg: &RowGroupMetaData) -> bool {
        let Some(RexType::ScalarFunction(sf)) = &expr.rex_type else {
            // Includes SingularOrList (IN): pruning it needs min/max, which is
            // not implemented yet, so it cannot rule anything out.
            return true;
        };
        let args: Vec<&Expression> = sf
            .arguments
            .iter()
            .filter_map(|a| match &a.arg_type {
                Some(function_argument::ArgType::Value(e)) => Some(e),
                _ => None,
            })
            .collect();
        match self.function_map.get(&sf.function_reference) {
            // AND cannot match if ANY conjunct is impossible for this group.
            Some(KnownFunction::And) => args.iter().all(|a| self.group_can_match(a, rg)),
            // OR needs only one possible disjunct.
            //
            // ENG-47483 R1 — but `any` on an EMPTY iterator is false, which
            // would prune the group. `select_row_groups` would then hand
            // `with_row_groups` an empty selection: no row groups fetched, no
            // rows returned, no error anywhere. An empty OR is malformed
            // Substrait, and `boolean_or` already refuses it at evaluation time
            // for precisely this reason — see its comment about silently
            // under-including at the RowFilter site. This is the same hazard one
            // layer coarser, so it takes the same answer: keep the group, and
            // let the evaluator raise and fall back.
            Some(KnownFunction::Or) if args.is_empty() => true,
            Some(KnownFunction::Or) => args.iter().any(|a| self.group_can_match(a, rg)),
            // Negation would invert every verdict below, and getting that wrong
            // drops live rows. Not worth the risk for the value: keep.
            Some(KnownFunction::Not) => true,
            Some(KnownFunction::IsNull) => match self.column_stats(&args, rg) {
                // No nulls in this group, so `IsNull` matches nothing.
                Some(st) => st.null_count_opt() != Some(0),
                None => true,
            },
            Some(KnownFunction::IsNotNull) => match self.column_stats(&args, rg) {
                // Every value null, so `IsNotNull` matches nothing.
                Some(st) => st.null_count_opt() != Some(rg.num_rows().max(0) as u64),
                None => true,
            },
            Some(
                &k @ (KnownFunction::Equal
                | KnownFunction::NotEqual
                | KnownFunction::Lt
                | KnownFunction::Lte
                | KnownFunction::Gt
                | KnownFunction::Gte),
            ) => self.comparison_can_match(k, &args, rg),
            _ => true,
        }
    }

    /// `true` if `rg` might contain a row where `col cmp literal` holds,
    /// judged against the column's min/max.
    ///
    /// Deliberately narrow. Only integer and UTF-8 string columns compared
    /// against an integer or string literal are pruned; everything else keeps
    /// the group. The excluded cases are excluded for reasons, not for effort:
    ///
    /// * **Floats** — parquet leaves NaN out of min/max, so a group containing
    ///   NaN has bounds that do not describe its contents.
    /// * **Decimal, Date, Timestamp** — the literal's scale or time unit has to
    ///   be reconciled with the column's physical encoding. The evaluator has
    ///   fiddly code for exactly this (`compare_timestamp_column`, rescaling),
    ///   and a mistake there is a wrong mask; the same mistake HERE is missing
    ///   rows, which is silent. Not worth replicating on this path.
    /// * **Deprecated stats** — older writers ordered byte arrays by signed
    ///   byte value, so `min`/`max` do not mean what a UTF-8 comparison
    ///   assumes.
    ///
    /// Truncated bounds are safe and are NOT declined: parquet truncates `min`
    /// down and `max` up, so the reported range is a superset of the real one
    /// and every rule below errs toward keeping the group.
    ///
    /// That widening direction is a WRITER contract (parquet-mr's
    /// `BinaryTruncator` and arrow-rs both honour it), and it is **not
    /// verifiable from the statistics** — do not try to "harden" this by
    /// consulting `Statistics::min_is_exact()` / `max_is_exact()`. Those read
    /// the optional `is_min_value_exact` / `is_max_value_exact` thrift fields,
    /// which parquet-58 decodes as `unwrap_or(false)`; most writers never emit
    /// them, so exactness reads false on ordinary untruncated stats and gating
    /// on it would silently decline every string comparison. Same trap as
    /// `is_min_max_backwards_compatible` above, one field over.
    fn comparison_can_match(
        &self,
        cmp: KnownFunction,
        args: &[&Expression],
        rg: &RowGroupMetaData,
    ) -> bool {
        if args.len() != 2 {
            return true;
        }
        // Which side is the column? Mirrors the evaluator's `reversed` flag.
        let col_is_lhs = matches!(args[0].rex_type, Some(RexType::Selection(_)));
        let lit_expr = if col_is_lhs { args[1] } else { args[0] };
        let Some(RexType::Literal(lit)) = &lit_expr.rex_type else {
            // Column-vs-column, or something we do not recognise.
            return true;
        };
        let Ok(scalar) = decode_literal(lit) else {
            return true;
        };
        let Some(st) = self.column_stats(args, rg) else {
            return true;
        };

        // A comparison never matches NULL, so an all-null group matches nothing
        // regardless of the operator or the bounds.
        if st.null_count_opt() == Some(rg.num_rows().max(0) as u64) {
            return false;
        }
        // Deprecated stats came from the legacy `min`/`max` thrift fields, which
        // used a signed byte sort order regardless of logical type. Unreliable
        // for byte arrays, so decline them for every type rather than reason
        // per-type about when signed ordering happens to be right.
        //
        // `is_min_max_backwards_compatible` is NOT a second guard: it is false
        // on ordinary modern stats (it only reports whether the deprecated
        // fields were ALSO written), so testing it would decline everything.
        if st.is_min_max_deprecated() {
            return true;
        }

        // Normalise to `column cmp literal`.
        let cmp = if col_is_lhs { cmp } else { mirror(cmp) };
        match bounds(st) {
            Some(Bounds::Int(lo, hi)) => match int_literal(&scalar) {
                Some(v) => range_can_match(cmp, lo, hi, v),
                None => true,
            },
            Some(Bounds::Str(lo, hi)) => match &scalar {
                ScalarValue::String(v) => {
                    range_can_match(cmp, lo.as_str(), hi.as_str(), v.as_str())
                }
                _ => true,
            },
            None => true,
        }
    }

    /// Statistics for the single column referenced by `args`, if exactly one
    /// column is referenced and the file carries stats for it.
    fn column_stats<'a>(
        &self,
        args: &[&Expression],
        rg: &'a RowGroupMetaData,
    ) -> Option<&'a parquet::file::statistics::Statistics> {
        // Folded rather than collected into a `BTreeSet`: `select_row_groups`
        // reaches here once per row group per predicate leaf, and the set was
        // allocated on every one of those calls only to be asked whether it
        // held exactly one element. Invisible against IO at row-group
        // granularity, but this walk is the one that would be reused at page
        // granularity, where the call count goes up by orders of magnitude.
        // `visit_field_indices` keeps it a single shared traversal, so this
        // cannot drift from `referenced_field_indices`.
        let mut sole: Option<usize> = None;
        let mut ambiguous = false;
        for a in args {
            Self::visit_field_indices(a, &mut |i| match sole {
                None => sole = Some(i),
                Some(seen) if seen != i => ambiguous = true,
                Some(_) => {}
            });
        }
        if ambiguous {
            return None;
        }
        let name = self.column_names.get(sole?)?;
        // ENG-47483 R2 — `column_descr().name()` is the LEAF name, not the
        // column path, and the comparison is case-insensitive. Two columns
        // differing only in case, or a nested field whose leaf collides with a
        // top-level name, would both match, and `find` would silently take
        // whichever came first — pruning this group on another column's
        // statistics.
        //
        // So require the match to be UNAMBIGUOUS, and require the column to be
        // top-level. `visit_field_indices` only ever yields top-level
        // struct-field references, so a column reached through a deeper path
        // cannot be the one the predicate named. Zero matches or several means
        // we cannot tell which column this is, and not being able to tell means
        // keep the group.
        let mut matching = rg.columns().iter().filter(|c| {
            c.column_descr().path().parts().len() == 1
                && c.column_descr().name().eq_ignore_ascii_case(name)
        });
        let col = matching.next()?;
        if matching.next().is_some() {
            return None;
        }
        col.statistics()
    }

    /// Evaluate the expression against a RecordBatch, returning one boolean
    /// per row. Caller passes the result to `arrow_select::filter_record_batch`.
    pub fn evaluate(&self, batch: &RecordBatch) -> Result<BooleanArray, String> {
        let result = self.eval(&self.expression, batch)?;
        result.into_bool_array(batch.num_rows())
    }
}

/// Column bounds in a form comparable to a decoded literal. Only the two
/// families [`PushedFilter::comparison_can_match`] is willing to prune on.
enum Bounds {
    Int(i64, i64),
    Str(String, String),
}

/// Widen the statistics we trust into [`Bounds`]. Returns `None` for every
/// other physical type, which keeps the row group.
fn bounds(st: &parquet::file::statistics::Statistics) -> Option<Bounds> {
    use parquet::file::statistics::Statistics as S;
    match st {
        S::Int32(v) => Some(Bounds::Int(
            i64::from(*v.min_opt()?),
            i64::from(*v.max_opt()?),
        )),
        S::Int64(v) => Some(Bounds::Int(*v.min_opt()?, *v.max_opt()?)),
        S::ByteArray(v) => {
            // Only when both bounds are valid UTF-8; a truncated multi-byte
            // char would otherwise compare as a different string.
            let lo = v.min_opt()?.as_utf8().ok()?.to_string();
            let hi = v.max_opt()?.as_utf8().ok()?.to_string();
            Some(Bounds::Str(lo, hi))
        }
        _ => None,
    }
}

/// Integer literals, widened. Non-integer literals return `None` and keep the
/// group; in particular a float literal against an integer column is NOT
/// coerced here, because rounding in the wrong direction drops live rows.
fn int_literal(s: &ScalarValue) -> Option<i64> {
    match s {
        ScalarValue::I8(v) => Some(i64::from(*v)),
        ScalarValue::I16(v) => Some(i64::from(*v)),
        ScalarValue::I32(v) => Some(i64::from(*v)),
        ScalarValue::I64(v) => Some(*v),
        _ => None,
    }
}

/// `a cmp b` with the operands swapped, so callers can normalise
/// `literal cmp column` into `column cmp literal`.
fn mirror(cmp: KnownFunction) -> KnownFunction {
    match cmp {
        KnownFunction::Lt => KnownFunction::Gt,
        KnownFunction::Lte => KnownFunction::Gte,
        KnownFunction::Gt => KnownFunction::Lt,
        KnownFunction::Gte => KnownFunction::Lte,
        other => other,
    }
}

/// Could any value in `[lo, hi]` satisfy `value cmp v`?
///
/// `NotEqual` can only be ruled out when the group is a single constant equal
/// to `v`; any wider range contains something else.
fn range_can_match<T: PartialOrd>(cmp: KnownFunction, lo: T, hi: T, v: T) -> bool {
    match cmp {
        KnownFunction::Equal => lo <= v && v <= hi,
        KnownFunction::NotEqual => !(lo == v && hi == v),
        KnownFunction::Lt => lo < v,
        KnownFunction::Lte => lo <= v,
        KnownFunction::Gt => hi > v,
        KnownFunction::Gte => hi >= v,
        _ => true,
    }
}

/// Filter a RecordBatch by a pushed predicate.
///
/// SQL three-valued logic: rows where the predicate evaluates to NULL are
/// dropped (treated as false), matching how Velox's `remainingFilterExprSet_`
/// behaves and what callers expect for WHERE-clause pushdown.
pub fn filter_batch(batch: &RecordBatch, filter: &PushedFilter) -> Result<RecordBatch, String> {
    let mask = filter.evaluate(batch)?;
    // arrow_select's filter_record_batch treats null mask entries as "drop",
    // which is what we want for SQL WHERE semantics.
    filter_record_batch(batch, &mask)
        .map_err(|e| format!("[ENG-40156] filter_record_batch failed: {e}"))
}

// ════════════════════════════════════════════════════════════════════════════
// Parquet-level pushdown adapter (ENG-42276)
// ════════════════════════════════════════════════════════════════════════════

/// Adapts a `PushedFilter` to parquet's `ArrowPredicate` trait so it can be
/// installed as a parquet `RowFilter` and drive row-group / page-index
/// pruning. Construct via `PushedFilter::build_row_filter`.
struct PushedFilterArrowPredicate {
    filter: PushedFilter,
    projection: ProjectionMask,
}

impl ArrowPredicate for PushedFilterArrowPredicate {
    fn projection(&self) -> &ProjectionMask {
        &self.projection
    }

    fn evaluate(&mut self, batch: RecordBatch) -> Result<BooleanArray, ArrowError> {
        match self.filter.evaluate(&batch) {
            Ok(mask) => Ok(mask),
            Err(e) => {
                // Don't propagate as an ArrowError — that would fail the
                // whole parquet read. Instead, return an all-true mask so
                // every row survives parquet-level filtering and the
                // post-merge filter in lib.rs evaluates the predicate
                // correctly. Net cost: pushdown's column-fetch work was
                // wasted, but correctness is preserved.
                // Deterministic for a given predicate + schema, and parquet
                // calls this per RecordBatch, so it would otherwise emit one
                // line per batch per row group per file.
                crate::warn_once!(
                    "[ENG-40156] parquet pushdown eval failed; emitting all-true \
                     mask so post-merge filter runs: {e}"
                );
                Ok(BooleanArray::from(vec![true; batch.num_rows()]))
            }
        }
    }
}

impl PushedFilter {
    /// Walk the expression tree and collect every substrait base-schema field
    /// index referenced by a `Selection`. Used to build the parquet projection
    /// mask for `RowFilter`.
    fn referenced_field_indices(&self) -> BTreeSet<usize> {
        let mut out = BTreeSet::new();
        Self::visit_field_indices(&self.expression, &mut |i| {
            out.insert(i);
        });
        out
    }

    /// The single traversal both reference-collecting callers share. Takes a
    /// sink rather than a `BTreeSet` so `column_stats` can fold without
    /// allocating on a per-row-group path, while this stays the only place
    /// that knows which node kinds carry a column reference — two copies of
    /// this walk could disagree about that, and the copy used by
    /// `select_row_groups` disagreeing means pruning on the wrong column.
    fn visit_field_indices(expr: &Expression, out: &mut impl FnMut(usize)) {
        match &expr.rex_type {
            Some(RexType::Selection(field_ref)) => {
                if let Some(field_reference::ReferenceType::DirectReference(seg)) =
                    &field_ref.reference_type
                    && let Some(reference_segment::ReferenceType::StructField(sf)) =
                        &seg.reference_type
                {
                    out(sf.field as usize);
                }
            }
            Some(RexType::ScalarFunction(sf)) => {
                for arg in &sf.arguments {
                    if let Some(function_argument::ArgType::Value(e)) = &arg.arg_type {
                        Self::visit_field_indices(e, out);
                    }
                }
            }
            // `x IN (a, b, c)` — the tested value carries the column reference;
            // options are normally literals but recurse anyway so a column
            // reference hiding in an option still shows up in the projection
            // mask and in the MOR primary-key gate.
            Some(RexType::SingularOrList(or_list)) => {
                if let Some(value) = &or_list.value {
                    Self::visit_field_indices(value, out);
                }
                for option in &or_list.options {
                    Self::visit_field_indices(option, out);
                }
            }
            _ => {}
        }
    }

    /// Walk the expression tree and collect every `function_reference` anchor a
    /// `ScalarFunction` node calls. `decode` validates only these anchors, so a
    /// plan-wide function table full of operators we can't evaluate (Gluten
    /// serialises the whole plan's table) doesn't cost us pushdown.
    ///
    /// A `SingularOrList` contributes no anchor of its own (IN is not a
    /// substrait function call) but its operands may, so we recurse through it.
    /// Rex variants we don't recognise at all are skipped rather than treated as
    /// an error — they can't contribute an anchor, and if they turn up in a
    /// pushed expression they fail later at eval time.
    fn collect_function_anchors(expr: &Expression, out: &mut BTreeSet<u32>) {
        match &expr.rex_type {
            Some(RexType::ScalarFunction(sf)) => {
                out.insert(sf.function_reference);
                for arg in &sf.arguments {
                    if let Some(function_argument::ArgType::Value(e)) = &arg.arg_type {
                        Self::collect_function_anchors(e, out);
                    }
                }
            }
            Some(RexType::SingularOrList(or_list)) => {
                if let Some(value) = &or_list.value {
                    Self::collect_function_anchors(value, out);
                }
                for option in &or_list.options {
                    Self::collect_function_anchors(option, out);
                }
            }
            _ => {}
        }
    }

    /// Walk the expression tree and return true iff at least one leaf is
    /// expected to reject enough rows to repay the second decode pass that
    /// installing a parquet `RowFilter` costs.
    ///
    /// # What the second pass costs and buys — decode, not I/O
    ///
    /// ENG-42276 v4.1 justified this gate as "column stats can't prune row
    /// groups for a null check", and called the price a "two-pass-read I/O
    /// cost". Both are wrong, and together they made the gate wrong for
    /// `IsNull` — see ENG-47480.
    ///
    /// With a `RowFilter` installed, parquet-rs decodes the predicate columns
    /// first, evaluates them into a `RowSelection`, then materialises the
    /// *remaining* projected columns only for the rows that survive. The same
    /// row groups are read either way and the predicate columns are read either
    /// way, so **no I/O is saved or added** — what the second pass trades is
    /// CPU: it skips decoding the remaining columns for rejected rows, and pays
    /// for the selection plus the loss of a straight sequential decode.
    ///
    /// Measured on TPC-DS 1TB (run `de2a9328`, round 2), which is why this is
    /// stated as fact rather than as the design intent: bytes read per row is
    /// flat whether a filter installs or not (10.60 vs 9.33 GiB per billion
    /// rows), while time per byte differs 7x (0.154 vs 1.045 s/GiB). Row-group
    /// pruning contributes nothing at all — `skipped row groups` was 0 across
    /// all 659 scan nodes.
    ///
    /// So the question is not "can stats skip a row group" but "will this
    /// predicate reject most rows".
    ///
    /// By that question the two null checks are opposites, not a pair:
    ///
    /// - `IsNotNull(x)` passes everything except the null fraction. On the
    ///   FK columns Spark attaches it to that fraction is 2-3% (measured on
    ///   TPC-DS 1TB), so it rejects almost nothing and the second pass is
    ///   waste. This is the q27/q29/q82/q84 regression v4.1 was filed for.
    /// - `IsNull(x)` is its complement: it keeps only that 2-3%. On TPC-DS
    ///   q76 it rejects 97.6-100.0% of rows, and declining it left 4.83B rows
    ///   to cross the FFI boundary and be re-filtered by Velox instead.
    ///
    /// The asymmetry below is therefore deliberate and load-bearing: it reads
    /// like an inconsistency, and it is not one.
    ///
    /// `negated` tracks whether an odd number of `Not` nodes encloses `expr`,
    /// because `Not(IsNull(x))` is `IsNotNull(x)` semantically and must be
    /// classified as the non-selective one. Comparison ops keep their verdict
    /// under negation (`Not(Equal)` is `NotEqual`, equally selective).
    fn is_worth_row_filtering(&self, expr: &Expression, negated: bool) -> bool {
        match &expr.rex_type {
            Some(RexType::ScalarFunction(sf)) => {
                let known = self.function_map.get(&sf.function_reference);
                match known {
                    Some(KnownFunction::Equal)
                    | Some(KnownFunction::NotEqual)
                    | Some(KnownFunction::Lt)
                    | Some(KnownFunction::Lte)
                    | Some(KnownFunction::Gt)
                    | Some(KnownFunction::Gte) => true,
                    Some(k @ (KnownFunction::And | KnownFunction::Or)) => {
                        // The two combinators are NOT symmetric here, and
                        // treating them as one was ENG-47483's R4.
                        //
                        // An AND is at least as selective as its most selective
                        // conjunct: every row must clear every branch, so one
                        // rejecting branch is enough to repay the pass. `any`.
                        //
                        // An OR is at most as selective as its LEAST selective
                        // branch: a row survives if either side lets it. Pairing
                        // a selective leaf with a permissive one yields a
                        // permissive predicate, so `WHERE a IS NULL OR b IS NOT
                        // NULL` must NOT install a filter — judging it on the
                        // `IsNull` alone reintroduces the ENG-42276 v4.1
                        // two-pass regression from the other direction. `all`.
                        //
                        // `negated` flips which one is in force (De Morgan):
                        // `Not(And(x, y))` is `Or(!x, !y)`, so an AND under an
                        // odd number of Nots is an effective OR and takes the
                        // `all` rule.
                        let effective_or = matches!(k, KnownFunction::Or) != negated;
                        let mut children =
                            sf.arguments.iter().filter_map(|arg| match &arg.arg_type {
                                Some(function_argument::ArgType::Value(child)) => Some(child),
                                _ => None,
                            });
                        if effective_or {
                            // `all` on an empty iterator is true, which reads as
                            // "worth filtering". That is the safe direction: the
                            // evaluator rejects an empty AND/OR with an Err and
                            // the caller falls back to an all-true mask, so the
                            // cost is one wasted pass, never a dropped row.
                            children.all(|c| self.is_worth_row_filtering(c, negated))
                        } else {
                            children.any(|c| self.is_worth_row_filtering(c, negated))
                        }
                    }
                    Some(KnownFunction::Not) => sf.arguments.iter().any(|arg| {
                        if let Some(function_argument::ArgType::Value(child)) = &arg.arg_type {
                            self.is_worth_row_filtering(child, !negated)
                        } else {
                            false
                        }
                    }),
                    // Selective: keeps only the null fraction. Under an odd
                    // number of Nots this is `IsNotNull`, which is not.
                    Some(KnownFunction::IsNull) => !negated,
                    // Non-selective: rejects only the null fraction. Under an
                    // odd number of Nots this is `IsNull`, which is selective.
                    Some(KnownFunction::IsNotNull) => negated,
                    None => {
                        // Unknown function — `decode()` would have dropped
                        // the whole filter already in this case, so this is
                        // defensive only.
                        false
                    }
                }
            }
            // `x IN (a, b, c)` is a disjunction of equalities, so parquet column
            // stats can prune any row group whose min/max range excludes every
            // option — prunable, exactly like a bare `equal`.
            Some(RexType::SingularOrList(_)) => true,
            _ => false,
        }
    }

    /// Build a parquet `RowFilter` that wraps this predicate.
    ///
    /// The returned `RowFilter` projects only the parquet columns this
    /// predicate references (so parquet can evaluate the predicate early
    /// and skip row groups / pages whose stats don't satisfy it before
    /// decoding the rest of the requested columns).
    ///
    /// Returns `None` when pushdown isn't safe / possible:
    /// - the expression references no fields (degenerate);
    /// - any referenced substrait column is not present in the parquet
    ///   file's top-level schema (column added in a newer schema version,
    ///   nested reference we don't translate, etc.);
    /// - **no leaf of the expression is expected to reject enough rows to
    ///   repay the second decode pass** — in practice a predicate built only from
    ///   `IsNotNull`/And/Or/Not, which passes everything but the 2-3% null
    ///   fraction. See ENG-42276 v4.1, and `is_worth_row_filtering` for why
    ///   `IsNull` is explicitly *not* in that set (ENG-47480).
    ///
    /// On `None`, the caller's existing post-merge filter still evaluates
    /// the predicate correctly — only the parquet-layer optimisation is
    /// skipped.
    pub fn build_row_filter(&self, parquet_schema: &SchemaDescriptor) -> Option<RowFilter> {
        let referenced = self.referenced_field_indices();
        if referenced.is_empty() {
            log::debug!(
                "[ENG-40156] pushdown: expression references no fields; \
                 skipping parquet RowFilter"
            );
            return None;
        }

        // ENG-42276 v4.1 / ENG-47480 — selectivity gate: skip pushdown when
        // no leaf rejects enough rows to repay the second decode pass.
        if !self.is_worth_row_filtering(&self.expression, false) {
            log::debug!(
                "[ENG-42276] pushdown: no leaf of the predicate is selective \
                 (IsNotNull/And/Or/Not only) — skipping parquet RowFilter to \
                 avoid paying a second decode pass that rejects almost nothing"
            );
            return None;
        }

        // Map substrait field index → column name → parquet root-column index.
        let root = parquet_schema.root_schema();
        let mut parquet_col_indices: Vec<usize> = Vec::with_capacity(referenced.len());
        for sub_idx in &referenced {
            let col_name = match self.column_names.get(*sub_idx) {
                Some(n) => n,
                None => {
                    // warn!, unlike the other misses in this function: the
                    // expression and `column_names` come from the same decoded
                    // payload, so an index outside it is a wire-format bug on the
                    // C++ side, not an expected schema difference. Pushdown is
                    // skipped either way, so this is diagnostic only.
                    crate::warn_once!(
                        "[ENG-40156] pushdown: substrait field index {sub_idx} out of \
                         range of column_names (len={}); skipping pushdown",
                        self.column_names.len()
                    );
                    return None;
                }
            };
            let mut found: Option<usize> = None;
            for (i, f) in root.get_fields().iter().enumerate() {
                if f.name() == col_name {
                    found = Some(i);
                    break;
                }
            }
            match found {
                Some(i) => parquet_col_indices.push(i),
                None => {
                    log::debug!(
                        "[ENG-40156] pushdown: column '{col_name}' not in parquet schema; \
                         skipping pushdown for this file"
                    );
                    return None;
                }
            }
        }

        let projection = ProjectionMask::roots(parquet_schema, parquet_col_indices);
        let predicate = PushedFilterArrowPredicate {
            filter: self.clone(),
            projection,
        };
        log::debug!(
            "[ENG-40156] pushdown: installing parquet RowFilter over cols {:?}",
            self.column_names
        );
        Some(RowFilter::new(vec![Box::new(predicate)]))
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Known function inventory
// ════════════════════════════════════════════════════════════════════════════

/// Substrait operators we can evaluate. Anything else → drop the predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KnownFunction {
    Equal,
    NotEqual,
    Lt,
    Lte,
    Gt,
    Gte,
    IsNull,
    IsNotNull,
    And,
    Or,
    Not,
}

impl KnownFunction {
    /// Resolve a substrait function declaration `name` like `"lt:any_any"` or
    /// `"is_null:any"` to a KnownFunction. The signature suffix is ignored —
    /// our evaluator dispatches on arrow DataType at evaluation time, not on
    /// the substrait signature.
    fn from_name(name: &str) -> Option<Self> {
        let base = name.split(':').next()?;
        Some(match base {
            "equal" | "eq" => Self::Equal,
            "not_equal" | "ne" | "neq" => Self::NotEqual,
            "lt" | "lessthan" | "less_than" => Self::Lt,
            "lte" | "lessthanorequal" | "less_than_or_equal" => Self::Lte,
            "gt" | "greaterthan" | "greater_than" => Self::Gt,
            "gte" | "greaterthanorequal" | "greater_than_or_equal" => Self::Gte,
            "is_null" | "isnull" => Self::IsNull,
            "is_not_null" | "isnotnull" => Self::IsNotNull,
            "and" | "and_kleene" => Self::And,
            "or" | "or_kleene" => Self::Or,
            "not" => Self::Not,
            _ => return None,
        })
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Evaluator
// ════════════════════════════════════════════════════════════════════════════

/// Intermediate evaluation result. Arrows (columns / boolean masks) and
/// scalars are interchangeable inputs to comparison ops; bool arrays propagate
/// through and/or/not.
#[derive(Debug, Clone)]
enum Value {
    /// Per-row boolean result (final output of comparisons or logical ops).
    Bool(BooleanArray),
    /// A column reference into the batch — typically the LHS of a comparison.
    Column(ArrayRef),
    /// A constant value — typically the RHS of a comparison.
    Scalar(ScalarValue),
}

#[derive(Debug, Clone)]
enum ScalarValue {
    Null,
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    String(String),
    /// Substrait Decimal literal: 16-byte little-endian two's-complement value
    /// plus declared precision/scale. Compared against arrow Decimal128 columns
    /// after rescaling to the column's scale.
    Decimal128 {
        value: i128,
        precision: i32,
        scale: i32,
    },
    /// Days since 1970-01-01 (Substrait Date / Arrow Date32).
    Date(i32),
    /// Nanoseconds since UNIX epoch — the canonical timestamp literal unit.
    ///
    /// Every Substrait timestamp literal (deprecated µs `Timestamp`, and
    /// `PrecisionTimestamp[Tz]` at s/ms/µs/ns) is normalised UP to nanoseconds
    /// at decode time, which is lossless. At comparison time the arrow column
    /// value is likewise scaled UP to nanoseconds, so neither side ever loses
    /// sub-unit digits (the previous µs canonical form truncated ns literals and
    /// µs literals against s/ms columns, silently shifting predicate boundaries).
    TimestampNanos(i64),
    /// Raw byte string (Substrait Binary / FixedBinary / Uuid).
    Binary(Vec<u8>),
}

impl Value {
    /// Force a Value into a BooleanArray (one row per batch row). Errors if
    /// the value isn't already a boolean column/array.
    fn into_bool_array(self, num_rows: usize) -> Result<BooleanArray, String> {
        match self {
            Value::Bool(b) => Ok(b),
            Value::Column(arr) => arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "[ENG-40156] expected boolean column, got {}",
                        arr.data_type()
                    )
                }),
            Value::Scalar(ScalarValue::Bool(b)) => Ok(BooleanArray::from(vec![b; num_rows])),
            other => Err(format!(
                "[ENG-40156] cannot coerce {other:?} to BooleanArray"
            )),
        }
    }
}

impl PushedFilter {
    fn eval(&self, expr: &Expression, batch: &RecordBatch) -> Result<Value, String> {
        match &expr.rex_type {
            Some(RexType::Literal(lit)) => Ok(Value::Scalar(decode_literal(lit)?)),
            Some(RexType::Selection(field_ref)) => {
                self.eval_field_ref(field_ref, batch).map(Value::Column)
            }
            Some(RexType::ScalarFunction(sf)) => self.eval_scalar_fn(sf, batch),
            Some(RexType::SingularOrList(or_list)) => {
                self.eval_singular_or_list(or_list, batch).map(Value::Bool)
            }
            other => Err(format!(
                "[ENG-40156] unsupported expression variant at evaluation: {other:?}"
            )),
        }
    }

    /// Evaluate `value IN (option, …)` — substrait's `SingularOrList`.
    ///
    /// Implemented as the Kleene OR-fold of vectorised equality comparisons, one
    /// per option (Gluten emits only short lists, and this keeps every option on
    /// the same kernel path as a bare `equal`). Folding with `or_kleene` gives
    /// SQL three-valued semantics for free: a definite match is TRUE even when
    /// another option is NULL, while a row that matches nothing is NULL — and so
    /// dropped — if the value or any option is NULL.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the tested value isn't a column, when an option isn't a
    /// literal, or when the option list is empty. An empty IN list would fold to
    /// an all-false mask, which at the parquet RowFilter would prune every row
    /// with no way to recover; erroring routes to the over-including fallbacks
    /// instead — the same reasoning as the empty AND/OR guards.
    fn eval_singular_or_list(
        &self,
        or_list: &SingularOrList,
        batch: &RecordBatch,
    ) -> Result<BooleanArray, String> {
        let value_expr = or_list
            .value
            .as_ref()
            .ok_or_else(|| "[ENG-40156] SingularOrList has no tested value".to_string())?;
        let column = match self.eval(value_expr, batch)? {
            Value::Column(arr) => arr,
            other => {
                return Err(format!(
                    "[ENG-40156] SingularOrList tested value must be a column, got {other:?}"
                ));
            }
        };
        if or_list.options.is_empty() {
            return Err("[ENG-40156] SingularOrList with no options".to_string());
        }

        let num_rows = batch.num_rows();
        let mut mask: Option<BooleanArray> = None;
        for option in &or_list.options {
            let literal = match &option.rex_type {
                Some(RexType::Literal(lit)) => decode_literal(lit)?,
                other => {
                    return Err(format!(
                        "[ENG-40156] SingularOrList option is not a literal: {other:?}"
                    ));
                }
            };
            let matches = compare_column_scalar(&column, Cmp::Eq, &literal, false, num_rows)?;
            mask = Some(match mask {
                None => matches,
                Some(acc) => boolean::or_kleene(&acc, &matches)
                    .map_err(|e| format!("[ENG-40156] IN or_kleene fold failed: {e}"))?,
            });
        }
        // `options` is non-empty, so at least one fold step ran.
        mask.ok_or_else(|| "[ENG-40156] SingularOrList produced no mask".to_string())
    }

    fn eval_field_ref(&self, fr: &FieldReference, batch: &RecordBatch) -> Result<ArrayRef, String> {
        // Only direct top-level struct field references are supported. Nested
        // struct / list / map access would require recursive descent we don't
        // need for any predicate Gluten currently emits to Hudi.
        let direct = match &fr.reference_type {
            Some(field_reference::ReferenceType::DirectReference(seg)) => seg,
            other => {
                return Err(format!(
                    "[ENG-40156] unsupported field reference type: {other:?}"
                ));
            }
        };
        let idx = match &direct.reference_type {
            Some(reference_segment::ReferenceType::StructField(sf)) => sf.field as usize,
            other => {
                return Err(format!(
                    "[ENG-40156] unsupported reference segment: {other:?}"
                ));
            }
        };
        let col_name = self.column_names.get(idx).ok_or_else(|| {
            format!(
                "[ENG-40156] field index {idx} out of bounds (base_schema has {} field(s))",
                self.column_names.len()
            )
        })?;
        let col = batch.column_by_name(col_name).ok_or_else(|| {
            let schema = batch.schema();
            let avail: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            format!(
                "[ENG-40156] referenced column '{col_name}' not in RecordBatch; \
                 available={avail:?}"
            )
        })?;
        Ok(col.clone())
    }

    fn eval_scalar_fn(&self, sf: &ScalarFunction, batch: &RecordBatch) -> Result<Value, String> {
        let known = self
            .function_map
            .get(&sf.function_reference)
            .ok_or_else(|| {
                // Shouldn't happen — decode() drops the filter if any anchor is unknown.
                format!(
                    "[ENG-40156] no KnownFunction for anchor {} at eval time \
                 (decode invariant violated)",
                    sf.function_reference
                )
            })?;

        // Walk arguments first — every argument must be an Expression Value.
        let args: Result<Vec<Value>, String> = sf
            .arguments
            .iter()
            .map(|a| match &a.arg_type {
                Some(function_argument::ArgType::Value(e)) => self.eval(e, batch),
                other => Err(format!(
                    "[ENG-40156] unsupported function argument type: {other:?}"
                )),
            })
            .collect();
        let args = args?;

        let n = batch.num_rows();
        match known {
            KnownFunction::And => boolean_and(args, n).map(Value::Bool),
            KnownFunction::Or => boolean_or(args, n).map(Value::Bool),
            KnownFunction::Not => boolean_not(args, n).map(Value::Bool),
            KnownFunction::IsNull => is_null(args).map(Value::Bool),
            KnownFunction::IsNotNull => is_not_null(args).map(Value::Bool),
            cmp => comparison(*cmp, args, n).map(Value::Bool),
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Literal decode
// ════════════════════════════════════════════════════════════════════════════

fn decode_literal(lit: &Literal) -> Result<ScalarValue, String> {
    match &lit.literal_type {
        Some(LiteralType::Boolean(b)) => Ok(ScalarValue::Bool(*b)),
        // Substrait packs i8 / i16 into i32 on the wire (proto int32). Narrow
        // here so we can downcast to Int8Array / Int16Array at compare time.
        Some(LiteralType::I8(v)) => Ok(ScalarValue::I8(*v as i8)),
        Some(LiteralType::I16(v)) => Ok(ScalarValue::I16(*v as i16)),
        Some(LiteralType::I32(v)) => Ok(ScalarValue::I32(*v)),
        Some(LiteralType::I64(v)) => Ok(ScalarValue::I64(*v)),
        Some(LiteralType::Fp32(v)) => Ok(ScalarValue::F32(*v)),
        Some(LiteralType::Fp64(v)) => Ok(ScalarValue::F64(*v)),
        Some(LiteralType::String(s)) => Ok(ScalarValue::String(s.clone())),
        // Substrait also has FixedChar / VarChar — both map to string columns
        // on the Arrow side, so we treat them as String here.
        Some(LiteralType::FixedChar(s)) => Ok(ScalarValue::String(s.clone())),
        Some(LiteralType::VarChar(vc)) => Ok(ScalarValue::String(vc.value.clone())),
        Some(LiteralType::Binary(b)) => Ok(ScalarValue::Binary(b.clone())),
        Some(LiteralType::FixedBinary(b)) => Ok(ScalarValue::Binary(b.clone())),
        Some(LiteralType::Uuid(b)) => Ok(ScalarValue::Binary(b.clone())),
        // Substrait Decimal literal: little-endian two's-complement bytes
        // (exactly 16 bytes) + precision + scale.
        Some(LiteralType::Decimal(d)) => {
            if d.value.len() != 16 {
                return Err(format!(
                    "[ENG-40156] Decimal literal has {} bytes, expected 16",
                    d.value.len()
                ));
            }
            let mut le = [0u8; 16];
            le.copy_from_slice(&d.value);
            Ok(ScalarValue::Decimal128 {
                value: i128::from_le_bytes(le),
                precision: d.precision,
                scale: d.scale,
            })
        }
        Some(LiteralType::Date(d)) => Ok(ScalarValue::Date(*d)),
        // Deprecated `Timestamp` is i64 µs since epoch. Keep it for now since
        // Gluten still emits it for spark TimestampType columns.
        #[allow(deprecated)]
        Some(LiteralType::Timestamp(ts)) => Ok(ScalarValue::TimestampNanos(
            ts.checked_mul(1_000)
                .ok_or_else(|| format!("[ENG-40156] Timestamp µs->ns overflow for {ts}"))?,
        )),
        // PrecisionTimestamp carries an explicit precision (0=s, 3=ms, 6=µs, 9=ns).
        // Normalise UP to ns at decode time (lossless); the column is scaled to
        // ns at compare time too.
        Some(LiteralType::PrecisionTimestamp(ts)) => Ok(ScalarValue::TimestampNanos(
            rescale_to_nanos(ts.value, ts.precision)?,
        )),
        Some(LiteralType::PrecisionTimestampTz(ts)) => Ok(ScalarValue::TimestampNanos(
            rescale_to_nanos(ts.value, ts.precision)?,
        )),
        Some(LiteralType::Null(_)) => Ok(ScalarValue::Null),
        None if lit.nullable => Ok(ScalarValue::Null),
        other => Err(format!("[ENG-40156] unsupported literal type: {other:?}")),
    }
}

/// Convert a Substrait PrecisionTimestamp value to nanoseconds (the canonical
/// literal unit). `precision` is the unit exponent: 0=seconds, 3=ms, 6=µs, 9=ns.
/// All conversions scale UP, so they are lossless (an overflow on an absurd
/// far-future value errors out rather than silently wrapping).
fn rescale_to_nanos(value: i64, precision: i32) -> Result<i64, String> {
    let factor: i64 = match precision {
        0 => 1_000_000_000,
        3 => 1_000_000,
        6 => 1_000,
        9 => 1,
        other => {
            return Err(format!(
                "[ENG-40156] unsupported PrecisionTimestamp precision: {other}"
            ));
        }
    };
    value.checked_mul(factor).ok_or_else(|| {
        format!("[ENG-40156] PrecisionTimestamp overflow rescaling {value} (precision {precision}) to ns")
    })
}

// ════════════════════════════════════════════════════════════════════════════
// Logical operators
// ════════════════════════════════════════════════════════════════════════════

/// SQL three-valued AND, via arrow's Kleene kernel: NULL acts as the identity
/// for AND when combined with TRUE, but propagates with FALSE on the other side.
fn boolean_and(args: Vec<Value>, n: usize) -> Result<BooleanArray, String> {
    // An empty AND/OR is malformed Substrait. Returning a degenerate mask here
    // silently under-includes at the parquet RowFilter site: an empty OR yields
    // all-false (prunes every row), and an empty AND yields all-true which a
    // wrapping NOT flips to all-false. Return Err instead — it routes through
    // the Err→all-true(+warn) fallback in PushedFilterArrowPredicate::evaluate
    // (SITE-1) and the unfiltered(+warn) fallback in lib.rs filter_batch caller
    // (SITE-2), so we over-include + warn rather than dropping rows.
    if args.is_empty() {
        return Err("[ENG-40156] and() with no arguments".to_string());
    }
    fold_kleene(args, n, boolean::and_kleene, "and")
}

fn boolean_or(args: Vec<Value>, n: usize) -> Result<BooleanArray, String> {
    // See boolean_and: an empty OR returning all-false would silently prune ALL
    // rows at the parquet RowFilter (unrecoverable under-include). Return Err so
    // the SITE-1/SITE-2 fallbacks over-include + warn instead.
    if args.is_empty() {
        return Err("[ENG-40156] or() with no arguments".to_string());
    }
    fold_kleene(args, n, boolean::or_kleene, "or")
}

/// Coerce every argument to a boolean mask and left-fold them with `kernel`.
/// `op` names the operator for error messages.
fn fold_kleene(
    args: Vec<Value>,
    n: usize,
    kernel: fn(&BooleanArray, &BooleanArray) -> Result<BooleanArray, ArrowError>,
    op: &str,
) -> Result<BooleanArray, String> {
    let mut iter = args.into_iter();
    // The empty case is rejected by the callers, so `next()` is always Some.
    let first = iter
        .next()
        .ok_or_else(|| format!("[ENG-40156] {op}() with no arguments"))?;
    let mut acc = first.into_bool_array(n)?;
    for value in iter {
        let next = value.into_bool_array(n)?;
        acc = kernel(&acc, &next).map_err(|e| format!("[ENG-40156] {op}() kernel failed: {e}"))?;
    }
    Ok(acc)
}

fn boolean_not(args: Vec<Value>, n: usize) -> Result<BooleanArray, String> {
    if args.len() != 1 {
        return Err(format!(
            "[ENG-40156] not() expects 1 argument, got {}",
            args.len()
        ));
    }
    let arr = args.into_iter().next().unwrap().into_bool_array(n)?;
    // arrow's `not` leaves null entries null — SQL's NOT NULL = NULL.
    boolean::not(&arr).map_err(|e| format!("[ENG-40156] not() kernel failed: {e}"))
}

// ════════════════════════════════════════════════════════════════════════════
// Null tests
// ════════════════════════════════════════════════════════════════════════════

/// Unwrap the single column argument of a null test. Every other operand shape
/// is degenerate (a null test over a constant or over a boolean sub-expression
/// result) and errors so the caller's safe fallback runs. `op` names the
/// operator for the error message.
fn null_test_column(args: Vec<Value>, op: &str) -> Result<ArrayRef, String> {
    if args.len() != 1 {
        return Err(format!(
            "[ENG-40156] {op}() expects 1 argument, got {}",
            args.len()
        ));
    }
    match args.into_iter().next().unwrap() {
        Value::Column(arr) => Ok(arr),
        // Scalar null test applied row-wise — but we don't know the row count
        // from a scalar alone. This shape is degenerate (filter collapses to a
        // constant). Caller would have folded this.
        Value::Scalar(ScalarValue::Null) => {
            Err(format!("[ENG-40156] {op} on a scalar without row context"))
        }
        Value::Scalar(_) => Err(format!(
            "[ENG-40156] {op} on a non-null scalar — should have been folded"
        )),
        Value::Bool(_) => Err(format!(
            "[ENG-40156] {op} on a boolean expression result — unusual; not supported"
        )),
    }
}

fn is_null(args: Vec<Value>) -> Result<BooleanArray, String> {
    let arr = null_test_column(args, "is_null")?;
    boolean::is_null(&arr).map_err(|e| format!("[ENG-40156] is_null() kernel failed: {e}"))
}

/// Unlike a comparison, the result of a null test is itself never null: every
/// row gets a definite TRUE/FALSE. arrow's `is_not_null` kernel matches that.
fn is_not_null(args: Vec<Value>) -> Result<BooleanArray, String> {
    let arr = null_test_column(args, "is_not_null")?;
    boolean::is_not_null(&arr).map_err(|e| format!("[ENG-40156] is_not_null() kernel failed: {e}"))
}

// ════════════════════════════════════════════════════════════════════════════
// Comparison operators
//
// Every comparison funnels through `apply_cmp`, which calls the vectorised
// `arrow_ord::cmp` kernel for the operator. The scalar operand is materialised
// as a one-element array **in the column's exact DataType** and wrapped in
// `Scalar`, so the kernel's own type checking replaces per-type downcast loops.
// Timestamps and decimals need a lossless unit/scale bridge first — see
// `compare_timestamp_column` / `compare_decimal_column`.
// ════════════════════════════════════════════════════════════════════════════

/// Largest scale/precision an Arrow `Decimal128` can carry.
const DECIMAL128_MAX_PRECISION: u8 = 38;

/// Cast options for the timestamp/decimal unit bridges. `safe: false` makes an
/// out-of-range value an error rather than a silent NULL: a NULL would drop the
/// row (under-include), whereas an error routes to the callers' over-including
/// fallbacks, which is the only safe direction here.
const LOSSLESS_CAST: CastOptions<'static> = CastOptions {
    safe: false,
    format_options: FormatOptions::new(),
};

#[derive(Debug, Clone, Copy)]
enum Cmp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl Cmp {
    fn from_known(k: KnownFunction) -> Option<Self> {
        Some(match k {
            KnownFunction::Equal => Cmp::Eq,
            KnownFunction::NotEqual => Cmp::Ne,
            KnownFunction::Lt => Cmp::Lt,
            KnownFunction::Lte => Cmp::Le,
            KnownFunction::Gt => Cmp::Gt,
            KnownFunction::Gte => Cmp::Ge,
            _ => return None,
        })
    }

    /// The `arrow_ord::cmp` kernel implementing this operator.
    fn kernel(self) -> fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError> {
        match self {
            Cmp::Eq => cmp::eq,
            Cmp::Ne => cmp::neq,
            Cmp::Lt => cmp::lt,
            Cmp::Le => cmp::lt_eq,
            Cmp::Gt => cmp::gt,
            Cmp::Ge => cmp::gt_eq,
        }
    }
}

/// Run `column [cmp] scalar` (or `scalar [cmp] column` when `reversed`) through
/// the vectorised arrow kernel. Both operands must already share a DataType.
///
/// Nulls propagate: the kernel yields NULL wherever the column is NULL, and the
/// callers treat NULL as "drop this row" (SQL WHERE semantics).
fn apply_cmp(
    cmp_op: Cmp,
    column: &dyn Datum,
    scalar: &dyn Datum,
    reversed: bool,
) -> Result<BooleanArray, String> {
    let (lhs, rhs) = if reversed {
        (scalar, column)
    } else {
        (column, scalar)
    };
    cmp_op.kernel()(lhs, rhs).map_err(|e| format!("[ENG-40156] comparison kernel failed: {e}"))
}

fn comparison(known: KnownFunction, args: Vec<Value>, n: usize) -> Result<BooleanArray, String> {
    let cmp = Cmp::from_known(known).ok_or_else(|| {
        format!("[ENG-40156] comparison() called with non-comparison function {known:?}")
    })?;
    if args.len() != 2 {
        return Err(format!(
            "[ENG-40156] {known:?} expects 2 arguments, got {}",
            args.len()
        ));
    }
    let mut iter = args.into_iter();
    let lhs = iter.next().unwrap();
    let rhs = iter.next().unwrap();

    match (lhs, rhs) {
        (Value::Column(c), Value::Scalar(s)) => compare_column_scalar(&c, cmp, &s, false, n),
        // Reverse direction: flip the comparator so a < b becomes b > a.
        (Value::Scalar(s), Value::Column(c)) => compare_column_scalar(&c, cmp, &s, true, n),
        (Value::Column(_), Value::Column(_)) => {
            // Possible to support but Gluten doesn't currently emit these to
            // Hudi (extractFiltersFromRemainingFilter only extracts
            // column-vs-literal predicates).
            Err("[ENG-40156] column-vs-column comparison not supported".to_string())
        }
        (Value::Scalar(a), Value::Scalar(b)) => Err(format!(
            "[ENG-40156] scalar-vs-scalar comparison ({a:?} vs {b:?}) — should be constant-folded"
        )),
        (Value::Bool(_), _) | (_, Value::Bool(_)) => {
            Err("[ENG-40156] comparison applied to a boolean expression result".to_string())
        }
    }
}

/// Apply `column [cmp] scalar` (or `scalar [cmp] column` if `reversed`).
///
/// Dispatches on the column's DataType only to build a matching one-element
/// scalar array (and, for timestamps / decimals, to bridge units and scales
/// losslessly); the comparison itself is always a vectorised arrow kernel.
///
/// Nulls in the column produce NULL in the result. The caller's outer
/// `filter_record_batch` treats NULL as DROP, matching SQL WHERE semantics.
///
/// # Errors
///
/// Returns `Err` when the scalar's type doesn't match the column's, when the
/// column type isn't comparable here, or when a unit/scale bridge would lose
/// or overflow a value. All of these route through the callers' safe fallbacks
/// (all-true mask at the parquet RowFilter, unfiltered batch post-merge).
fn compare_column_scalar(
    col: &ArrayRef,
    cmp: Cmp,
    scalar: &ScalarValue,
    reversed: bool,
    n: usize,
) -> Result<BooleanArray, String> {
    debug_assert_eq!(col.len(), n);

    match col.data_type() {
        // Substrait timestamp literals are canonical nanoseconds; the column may
        // use any TimeUnit, so scale the COLUMN up to ns (lossless) rather than
        // the literal down (lossy).
        DataType::Timestamp(unit, _) => {
            compare_timestamp_column(col, cmp, scalar, *unit, reversed, n)
        }
        // Decimal literals carry their own scale, which may be finer than the
        // column's — compare at the finer of the two, scaling UP only.
        DataType::Decimal128(p, s) => compare_decimal_column(col, cmp, scalar, *p, *s, reversed, n),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Boolean
        | DataType::Date32
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary => {
            if matches!(scalar, ScalarValue::Null) {
                // Comparison with NULL → NULL for every row.
                return Ok(BooleanArray::from(vec![None; n]));
            }
            let literal = scalar_array(col.data_type(), scalar)?;
            // ENG-47570 — see `normalize_signed_zeros`. A no-op for every
            // column type and every literal except a float against ±0.0.
            let (column, literal) = normalize_signed_zeros(col.clone(), literal);
            apply_cmp(cmp, &column, &Scalar::new(&literal), reversed)
        }
        other => Err(format!(
            "[ENG-40156] comparison on unsupported column type {other}"
        )),
    }
}

/// Map `-0.0` to `0.0` on both operands when the literal is a zero, so float
/// comparisons match Spark rather than IEEE-754 totalOrder.
///
/// Arrow's kernels order floats by totalOrder — `is_eq` is literally
/// `to_bits() == to_bits()` (`arrow-array/src/arithmetic.rs`) — under which
/// `-0.0` and `0.0` are distinct and `-0.0` sorts strictly below `0.0`. Spark
/// compares after `NormalizeNaNAndZero`, and its `EqualTo` on doubles generates
/// `(isNaN(a) && isNaN(b)) || a == b`, so it holds `-0.0 = 0.0` true.
///
/// Left uncorrected, `c = 0.0` and `c >= 0.0` **drop** a row holding `-0.0`.
/// That is the one direction this module must never fail in: every other error
/// path here over-includes (an all-true mask at the parquet `RowFilter`, the
/// unfiltered batch post-merge) because `requiresPostScanFilterReeval()` is
/// unconditionally true and Velox re-evaluates the predicate on whatever we
/// return — which can only remove further rows, never restore a dropped one.
///
/// # Why the literal-is-zero gate is exact, not a heuristic
///
/// `-0.0` and `0.0` compare identically against every value except each other:
/// both are greater than any negative and less than any positive, and
/// `total_cmp` separates them only at the zero boundary. So the correction is
/// needed exactly when the literal is a zero, and skipping it elsewhere costs
/// no correctness — it avoids a pass over the column on the common path.
///
/// # Why NaN is deliberately untouched
///
/// Arrow's totalOrder already makes `NaN = NaN` true and orders `NaN` above
/// every non-NaN, which is what Spark does. The per-row `PartialOrd` loop this
/// module used before vectorization did neither, so the kernels *fixed* two
/// NaN under-includes. `v == 0.0` is false for NaN, so nothing below touches it.
fn normalize_signed_zeros(column: ArrayRef, literal: ArrayRef) -> (ArrayRef, ArrayRef) {
    macro_rules! normalize {
        ($arr_ty:ty, $native:ty) => {{
            let literal_value = match literal.as_any().downcast_ref::<$arr_ty>() {
                // `scalar_array` always builds exactly one non-null element;
                // anything else means the operand didn't come from there, so
                // leave both sides alone rather than guess.
                Some(a) if a.len() == 1 && !a.is_null(0) => a.value(0),
                _ => return (column, literal),
            };
            // True for both zeros, false for NaN.
            if literal_value != 0.0 {
                return (column, literal);
            }
            let Some(column_values) = column.as_any().downcast_ref::<$arr_ty>() else {
                return (column, literal);
            };
            // `unary` preserves the null buffer, so NULLs stay NULL.
            let normalized: $arr_ty =
                unary(column_values, |v: $native| if v == 0.0 { 0.0 } else { v });
            let zero: $arr_ty = <$arr_ty>::from(vec![0.0 as $native]);
            (Arc::new(normalized) as ArrayRef, Arc::new(zero) as ArrayRef)
        }};
    }
    match literal.data_type() {
        DataType::Float32 => normalize!(Float32Array, f32),
        DataType::Float64 => normalize!(Float64Array, f64),
        _ => (column, literal),
    }
}

/// Materialise `scalar` as a one-element array whose DataType is exactly
/// `data_type`, so `arrow_ord::cmp` can compare it against a column of that
/// type without any coercion.
///
/// Timestamp and decimal columns are handled by their own bridges and are not
/// accepted here.
///
/// # Errors
///
/// Returns `Err` if the scalar's type doesn't correspond to `data_type` — a
/// mismatch must never panic, it must route to the callers' safe fallbacks.
fn scalar_array(data_type: &DataType, scalar: &ScalarValue) -> Result<ArrayRef, String> {
    let array: ArrayRef = match (data_type, scalar) {
        (DataType::Int8, ScalarValue::I8(v)) => Arc::new(Int8Array::from(vec![*v])),
        (DataType::Int16, ScalarValue::I16(v)) => Arc::new(Int16Array::from(vec![*v])),
        (DataType::Int32, ScalarValue::I32(v)) => Arc::new(Int32Array::from(vec![*v])),
        (DataType::Int64, ScalarValue::I64(v)) => Arc::new(Int64Array::from(vec![*v])),
        (DataType::Float32, ScalarValue::F32(v)) => Arc::new(Float32Array::from(vec![*v])),
        (DataType::Float64, ScalarValue::F64(v)) => Arc::new(Float64Array::from(vec![*v])),
        (DataType::Boolean, ScalarValue::Bool(v)) => Arc::new(BooleanArray::from(vec![*v])),
        (DataType::Date32, ScalarValue::Date(v)) => Arc::new(Date32Array::from(vec![*v])),
        // Utf8 uses i32 offsets (StringArray), LargeUtf8 i64 (LargeStringArray);
        // the kernel rejects a mismatch, so each gets its own array type.
        (DataType::Utf8, ScalarValue::String(s)) => Arc::new(StringArray::from(vec![s.as_str()])),
        (DataType::LargeUtf8, ScalarValue::String(s)) => {
            Arc::new(LargeStringArray::from(vec![s.as_str()]))
        }
        // A string literal against a binary column compares as its UTF-8 bytes,
        // which is the same byte-lexical order arrow's binary kernel uses.
        (DataType::Binary, ScalarValue::Binary(b)) => {
            Arc::new(BinaryArray::from(vec![b.as_slice()]))
        }
        (DataType::Binary, ScalarValue::String(s)) => {
            Arc::new(BinaryArray::from(vec![s.as_bytes()]))
        }
        (DataType::LargeBinary, ScalarValue::Binary(b)) => {
            Arc::new(LargeBinaryArray::from(vec![b.as_slice()]))
        }
        (DataType::LargeBinary, ScalarValue::String(s)) => {
            Arc::new(LargeBinaryArray::from(vec![s.as_bytes()]))
        }
        (_, other) => {
            return Err(format!(
                "[ENG-40156] type mismatch: column is {data_type} but scalar is {other:?}"
            ));
        }
    };
    Ok(array)
}

/// Compare an Arrow Timestamp column against a Substrait timestamp scalar.
///
/// Substrait literals are canonical nanoseconds (see
/// `ScalarValue::TimestampNanos`). The column may use any TimeUnit, so the
/// COLUMN is cast up to nanoseconds — never the literal down, which would
/// truncate sub-unit digits and silently shift the predicate boundary (e.g.
/// `ts >= 1.5s` against a second-resolution column must not become `ts >= 1s`).
///
/// The cast runs with `safe: false` so an overflow (a far-future value that
/// doesn't fit i64 nanoseconds) errors out instead of becoming NULL — a NULL
/// would drop the row, whereas an error routes to the safe over-including
/// fallbacks.
fn compare_timestamp_column(
    col: &ArrayRef,
    cmp: Cmp,
    scalar: &ScalarValue,
    unit: TimeUnit,
    reversed: bool,
    n: usize,
) -> Result<BooleanArray, String> {
    let nanos = match scalar {
        ScalarValue::TimestampNanos(v) => *v,
        ScalarValue::Null => return Ok(BooleanArray::from(vec![None; n])),
        other => {
            return Err(format!(
                "[ENG-40156] type mismatch: column is Timestamp but scalar is {other:?}"
            ));
        }
    };
    let timezone = match col.data_type() {
        DataType::Timestamp(_, tz) => tz.clone(),
        other => {
            return Err(format!(
                "[ENG-40156] expected Timestamp column, got {other}"
            ));
        }
    };

    // Same timezone on both sides: casting only changes resolution, never the
    // instant, so no timezone conversion is involved.
    let target = DataType::Timestamp(TimeUnit::Nanosecond, timezone.clone());
    let column_nanos: ArrayRef = if unit == TimeUnit::Nanosecond {
        col.clone()
    } else {
        cast_with_options(col, &target, &LOSSLESS_CAST)
            .map_err(|e| format!("[ENG-40156] Timestamp ->ns cast failed: {e}"))?
    };
    let literal = TimestampNanosecondArray::from(vec![nanos]).with_timezone_opt(timezone);
    apply_cmp(cmp, &column_nanos, &Scalar::new(&literal), reversed)
}

/// Compare an Arrow Decimal128 column against a Substrait Decimal scalar.
///
/// Both sides are compared at the FINER scale = max(column scale, literal
/// scale), scaling each side UP — never dividing the literal down, which would
/// truncate it (`v = 0.5` against a `Decimal(_, 0)` column must match nothing,
/// not degrade into `v = 0`). Scaling up is lossless; the column is cast to the
/// maximum precision so the rescaled literal always fits the same DataType the
/// kernel requires on both sides.
fn compare_decimal_column(
    col: &ArrayRef,
    cmp: Cmp,
    scalar: &ScalarValue,
    col_precision: u8,
    col_scale: i8,
    reversed: bool,
    n: usize,
) -> Result<BooleanArray, String> {
    let (scalar_value, _scalar_precision, scalar_scale) = match scalar {
        ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        } => (*value, *precision, *scale),
        ScalarValue::Null => return Ok(BooleanArray::from(vec![None; n])),
        other => {
            return Err(format!(
                "[ENG-40156] type mismatch: column is Decimal128 but scalar is {other:?}"
            ));
        }
    };

    let common_scale = (col_scale as i32).max(scalar_scale);
    if common_scale > DECIMAL128_MAX_PRECISION as i32 {
        return Err(format!(
            "[ENG-40156] Decimal common scale {common_scale} exceeds Decimal128 limit"
        ));
    }
    let scale_up_factor = |from: i32, to: i32| -> Result<i128, String> {
        let exp = to - from;
        10i128
            .checked_pow(exp as u32)
            .ok_or_else(|| format!("[ENG-40156] Decimal rescale overflow: 10^{exp}"))
    };
    let rescaled_literal = scalar_value
        .checked_mul(scale_up_factor(scalar_scale, common_scale)?)
        .ok_or_else(|| "[ENG-40156] Decimal rescale value overflow".to_string())?;

    let target = DataType::Decimal128(DECIMAL128_MAX_PRECISION, common_scale as i8);
    let column_rescaled: ArrayRef =
        if col_precision == DECIMAL128_MAX_PRECISION && common_scale == col_scale as i32 {
            col.clone()
        } else {
            cast_with_options(col, &target, &LOSSLESS_CAST)
                .map_err(|e| format!("[ENG-40156] Decimal rescale cast failed: {e}"))?
        };
    let literal = Decimal128Array::from(vec![rescaled_literal])
        .with_precision_and_scale(DECIMAL128_MAX_PRECISION, common_scale as i8)
        .map_err(|e| format!("[ENG-40156] Decimal literal does not fit the column scale: {e}"))?;
    apply_cmp(cmp, &column_rescaled, &Scalar::new(&literal), reversed)
}

// ════════════════════════════════════════════════════════════════════════════
// Tests
// ════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    use substrait::proto::{
        Expression, ExtendedExpression, FunctionArgument, NamedStruct, Type,
        expression::{
            FieldReference, Literal, ReferenceSegment, ScalarFunction, field_reference,
            literal::LiteralType, reference_segment,
        },
        expression_reference,
        extensions::{
            SimpleExtensionDeclaration, SimpleExtensionUri, simple_extension_declaration,
        },
        function_argument, r#type,
    };

    // ─── Helpers ────────────────────────────────────────────────────────────

    fn make_batch_two_int_cols() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let a = Int64Array::from(vec![Some(1), Some(2), Some(3), None, Some(5)]);
        let b = Int64Array::from(vec![Some(10), Some(20), Some(30), Some(40), Some(50)]);
        RecordBatch::try_new(schema, vec![Arc::new(a), Arc::new(b)]).unwrap()
    }

    fn make_batch_with_string() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let s = StringArray::from(vec![Some("apple"), Some("banana"), None, Some("cherry")]);
        RecordBatch::try_new(schema, vec![Arc::new(s)]).unwrap()
    }

    /// C4: a LargeUtf8 column (LargeStringArray, i64 offsets) must compare
    /// correctly. Before the fix the LargeUtf8 arm downcast to StringArray and
    /// failed at runtime for every LargeUtf8 column. Drive `compare_column_scalar`
    /// directly so the assertion pins the downcast path.
    #[test]
    fn test_compare_large_utf8_column_downcasts_correctly() {
        use arrow_array::LargeStringArray;
        let col: ArrayRef = Arc::new(LargeStringArray::from(vec![
            Some("apple"),
            Some("banana"),
            None,
            Some("cherry"),
        ]));
        let scalar = ScalarValue::String("banana".to_string());
        let out =
            compare_column_scalar(&col, Cmp::Eq, &scalar, false, col.len()).expect("LargeUtf8 eq");
        assert!(!out.value(0), "apple != banana");
        assert!(out.value(1), "banana == banana");
        assert!(out.is_null(2), "null row stays null");
        assert!(!out.value(3), "cherry != banana");
    }

    fn named_struct(names: &[&str]) -> NamedStruct {
        NamedStruct {
            names: names.iter().map(|s| s.to_string()).collect(),
            r#struct: Some(r#type::Struct::default()),
        }
    }

    fn col_ref(field_idx: i32) -> Expression {
        Expression {
            rex_type: Some(RexType::Selection(Box::new(FieldReference {
                reference_type: Some(field_reference::ReferenceType::DirectReference(
                    ReferenceSegment {
                        reference_type: Some(reference_segment::ReferenceType::StructField(
                            Box::new(reference_segment::StructField {
                                field: field_idx,
                                child: None,
                            }),
                        )),
                    },
                )),
                root_type: None,
            }))),
        }
    }

    fn i64_literal(v: i64) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::I64(v)),
            })),
        }
    }

    fn string_literal(s: &str) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::String(s.to_string())),
            })),
        }
    }

    fn null_literal() -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: true,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::Null(Type {
                    kind: Some(r#type::Kind::I64(r#type::I64::default())),
                })),
            })),
        }
    }

    fn scalar_fn(anchor: u32, args: Vec<Expression>) -> Expression {
        Expression {
            rex_type: Some(RexType::ScalarFunction(ScalarFunction {
                function_reference: anchor,
                arguments: args
                    .into_iter()
                    .map(|e| FunctionArgument {
                        arg_type: Some(function_argument::ArgType::Value(e)),
                    })
                    .collect(),
                output_type: None,
                ..Default::default()
            })),
        }
    }

    /// Build an ExtendedExpression with a single named function declaration.
    fn extended(functions: &[(u32, &str)], names: &[&str], expr: Expression) -> Vec<u8> {
        let extension_uris = vec![SimpleExtensionUri {
            extension_uri_anchor: 1,
            uri: "/functions_comparison.yaml".to_string(),
        }];
        let extensions: Vec<SimpleExtensionDeclaration> = functions
            .iter()
            .map(|(anchor, name)| SimpleExtensionDeclaration {
                mapping_type: Some(
                    simple_extension_declaration::MappingType::ExtensionFunction(
                        simple_extension_declaration::ExtensionFunction {
                            extension_uri_reference: 1,
                            function_anchor: *anchor,
                            name: name.to_string(),
                        },
                    ),
                ),
            })
            .collect();
        let ext = ExtendedExpression {
            version: None,
            extension_uris,
            extensions,
            referred_expr: vec![substrait::proto::ExpressionReference {
                output_names: vec!["filter".to_string()],
                expr_type: Some(expression_reference::ExprType::Expression(expr)),
            }],
            base_schema: Some(named_struct(names)),
            advanced_extensions: None,
            expected_type_urls: vec![],
        };
        let mut buf = Vec::new();
        ext.encode(&mut buf).unwrap();
        buf
    }

    // ─── Test cases ─────────────────────────────────────────────────────────

    #[test]
    fn empty_bytes_decode_to_none() {
        let res = PushedFilter::decode(&[]).unwrap();
        assert!(res.is_none(), "empty bytes should produce None");
    }

    #[test]
    fn decode_round_trip_lt() {
        let bytes = extended(
            &[(42, "lt:any_any")],
            &["a", "b"],
            scalar_fn(42, vec![col_ref(0), i64_literal(3)]),
        );
        let pf = PushedFilter::decode(&bytes)
            .unwrap()
            .expect("should decode");
        assert_eq!(pf.column_names, vec!["a", "b"]);
        assert_eq!(pf.function_map.get(&42), Some(&KnownFunction::Lt));
    }

    #[test]
    fn unknown_function_drops_predicate() {
        // function name not in the inventory → decode returns None.
        let bytes = extended(
            &[(7, "weird_custom_func:any")],
            &["a"],
            scalar_fn(7, vec![col_ref(0)]),
        );
        let res = PushedFilter::decode(&bytes).unwrap();
        assert!(res.is_none(), "unknown function should drop entire filter");
    }

    #[test]
    fn lt_filter_excludes_matching_rows() {
        let batch = make_batch_two_int_cols();
        let bytes = extended(
            &[(1, "lt:any_any")],
            &["a", "b"],
            scalar_fn(1, vec![col_ref(0), i64_literal(3)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let mask = pf.evaluate(&batch).unwrap();
        // a < 3 → rows 0(=1), 1(=2) match; row 3(=NULL) → NULL; others false
        assert!(mask.value(0));
        assert!(mask.value(1));
        assert!(!mask.value(2));
        assert!(
            mask.is_null(3),
            "NULL in column should produce NULL in mask"
        );
        assert!(!mask.value(4));
    }

    #[test]
    fn and_combines_two_comparisons() {
        let batch = make_batch_two_int_cols();
        // a > 1 AND b < 40
        let bytes = extended(
            &[(1, "and:bool_bool"), (2, "gt:any_any"), (3, "lt:any_any")],
            &["a", "b"],
            scalar_fn(
                1,
                vec![
                    scalar_fn(2, vec![col_ref(0), i64_literal(1)]),
                    scalar_fn(3, vec![col_ref(1), i64_literal(40)]),
                ],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let filtered = filter_batch(&batch, &pf).unwrap();
        // a > 1 → rows 1, 2, 4 (row 3 is NULL → drops). b < 40 → rows 0, 1, 2.
        // AND → rows 1, 2.
        assert_eq!(filtered.num_rows(), 2);
        let a = filtered
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(a.value(0), 2);
        assert_eq!(a.value(1), 3);
    }

    #[test]
    fn or_keeps_rows_matching_either() {
        let batch = make_batch_two_int_cols();
        // a = 1 OR b = 50
        let bytes = extended(
            &[
                (1, "or:bool_bool"),
                (2, "equal:any_any"),
                (3, "equal:any_any"),
            ],
            &["a", "b"],
            scalar_fn(
                1,
                vec![
                    scalar_fn(2, vec![col_ref(0), i64_literal(1)]),
                    scalar_fn(3, vec![col_ref(1), i64_literal(50)]),
                ],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let filtered = filter_batch(&batch, &pf).unwrap();
        // row 0: a=1 → match. row 4: b=50 → match. row 3 (a=NULL, b=40) → both
        // NULL/false → drop.
        assert_eq!(filtered.num_rows(), 2);
    }

    #[test]
    fn not_inverts_mask() {
        let batch = make_batch_two_int_cols();
        // NOT (a = 1)
        let bytes = extended(
            &[(1, "not:bool"), (2, "equal:any_any")],
            &["a", "b"],
            scalar_fn(1, vec![scalar_fn(2, vec![col_ref(0), i64_literal(1)])]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let mask = pf.evaluate(&batch).unwrap();
        assert!(!mask.value(0));
        assert!(mask.value(1));
        assert!(mask.value(2));
        assert!(mask.is_null(3));
        assert!(mask.value(4));
    }

    #[test]
    fn is_null_matches_null_rows() {
        let batch = make_batch_two_int_cols();
        let bytes = extended(
            &[(1, "is_null:any")],
            &["a", "b"],
            scalar_fn(1, vec![col_ref(0)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let mask = pf.evaluate(&batch).unwrap();
        assert!(!mask.value(0));
        assert!(mask.value(3));
    }

    #[test]
    fn string_equality_filter() {
        let batch = make_batch_with_string();
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["s"],
            scalar_fn(1, vec![col_ref(0), string_literal("banana")]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let filtered = filter_batch(&batch, &pf).unwrap();
        assert_eq!(filtered.num_rows(), 1);
        let s = filtered
            .column_by_name("s")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(s.value(0), "banana");
    }

    #[test]
    fn null_literal_in_comparison_makes_mask_null() {
        let batch = make_batch_two_int_cols();
        // a > NULL → always NULL → caller drops all rows.
        let bytes = extended(
            &[(1, "gt:any_any")],
            &["a", "b"],
            scalar_fn(1, vec![col_ref(0), null_literal()]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let mask = pf.evaluate(&batch).unwrap();
        for i in 0..mask.len() {
            assert!(mask.is_null(i), "row {i} should be NULL");
        }
        // filter_record_batch drops all NULL → empty batch.
        let filtered = filter_batch(&batch, &pf).unwrap();
        assert_eq!(filtered.num_rows(), 0);
    }

    #[test]
    fn reversed_operand_order_works() {
        let batch = make_batch_two_int_cols();
        // literal < a (literal on LHS) — exercises the reversed path
        let bytes = extended(
            &[(1, "lt:any_any")],
            &["a", "b"],
            scalar_fn(1, vec![i64_literal(2), col_ref(0)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let mask = pf.evaluate(&batch).unwrap();
        // 2 < a → matches when a > 2 → rows 2(=3), 4(=5). Row 3 NULL.
        assert!(!mask.value(0));
        assert!(!mask.value(1));
        assert!(mask.value(2));
        assert!(mask.is_null(3));
        assert!(mask.value(4));
    }

    #[test]
    fn missing_column_in_batch_is_error() {
        // base_schema says "a","b","c" but batch only has "a","b" → evaluating
        // a reference to c yields an error (decode succeeds — it can't know).
        let batch = make_batch_two_int_cols();
        let bytes = extended(
            &[(1, "lt:any_any")],
            &["a", "b", "c"],
            scalar_fn(1, vec![col_ref(2), i64_literal(0)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let err = pf.evaluate(&batch).unwrap_err();
        assert!(err.contains("not in RecordBatch"), "got: {err}");
    }

    #[test]
    fn nested_and_with_or() {
        let batch = make_batch_two_int_cols();
        // (a > 1 AND a < 5) OR b = 10
        let bytes = extended(
            &[
                (1, "or:bool_bool"),
                (2, "and:bool_bool"),
                (3, "gt:any_any"),
                (4, "lt:any_any"),
                (5, "equal:any_any"),
            ],
            &["a", "b"],
            scalar_fn(
                1,
                vec![
                    scalar_fn(
                        2,
                        vec![
                            scalar_fn(3, vec![col_ref(0), i64_literal(1)]),
                            scalar_fn(4, vec![col_ref(0), i64_literal(5)]),
                        ],
                    ),
                    scalar_fn(5, vec![col_ref(1), i64_literal(10)]),
                ],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let filtered = filter_batch(&batch, &pf).unwrap();
        // Rows where (1 < a < 5) OR (b=10):
        //   row 0: a=1 → 1<1<5 false. b=10 → true. KEEP
        //   row 1: a=2 → 1<2<5 true. KEEP
        //   row 2: a=3 → true. KEEP
        //   row 3: a=NULL → NULL in inner AND. b=40 → b=10 false. OR(NULL,false)=NULL. DROP
        //   row 4: a=5 → 5<5 false. b=50 → false. DROP
        assert_eq!(filtered.num_rows(), 3);
    }

    // ════════════════════════════════════════════════════════════════════════
    // Type-coverage tests for ENG-40156. These mirror the column types that
    // TestMORFileSliceLayouts uses (id Int32, col_string, col_bigint Int64,
    // col_smallint Int16, col_tinyint Int8, col_float, col_double, col_boolean,
    // col_decimal(10,2), col_timestamp, col_date, col_binary) and the filter
    // shapes Gluten emits for `cast(... AS T) = literal`, `>`/`<`, `IS NULL`,
    // and compound AND/OR/NOT. Each test builds the same Substrait protobuf
    // a real query would, prost-encodes it, then decodes + evaluates.
    // ════════════════════════════════════════════════════════════════════════

    use arrow_array::{
        BinaryArray, BooleanArray as ArrBoolArr, Date32Array, Decimal128Array, Float32Array,
        Float64Array, Int8Array, Int16Array, Int32Array, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    };
    use substrait::proto::expression::literal::{Decimal, PrecisionTimestamp, VarChar};
    use substrait::proto::r#type as ptype;

    fn i32_literal(v: i32) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::I32(v)),
            })),
        }
    }
    fn i16_literal(v: i16) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::I16(v as i32)),
            })),
        }
    }
    fn i8_literal(v: i8) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::I8(v as i32)),
            })),
        }
    }
    fn f32_literal(v: f32) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::Fp32(v)),
            })),
        }
    }
    fn f64_literal(v: f64) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::Fp64(v)),
            })),
        }
    }
    fn bool_literal(v: bool) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::Boolean(v)),
            })),
        }
    }
    fn date_literal(days: i32) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::Date(days)),
            })),
        }
    }
    fn ts_micros_literal(micros: i64) -> Expression {
        // Use deprecated Timestamp form (still emitted by Gluten).
        #[allow(deprecated)]
        let lt = LiteralType::Timestamp(micros);
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(lt),
            })),
        }
    }
    fn precision_ts_literal(value: i64, precision: i32) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::PrecisionTimestamp(PrecisionTimestamp {
                    value,
                    precision,
                })),
            })),
        }
    }
    fn binary_literal(bytes: &[u8]) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::Binary(bytes.to_vec())),
            })),
        }
    }
    fn varchar_literal(s: &str) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::VarChar(VarChar {
                    value: s.to_string(),
                    length: s.len() as u32,
                })),
            })),
        }
    }
    fn decimal_literal(value: i128, precision: i32, scale: i32) -> Expression {
        let bytes = value.to_le_bytes().to_vec();
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::Decimal(Decimal {
                    value: bytes,
                    precision,
                    scale,
                })),
            })),
        }
    }

    // ─── Int8 / Int16 / Int32 / Float32 / Float64 / Bool ──────────────────

    #[test]
    fn int8_column_equality_filter() {
        let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int8, true)]));
        let a = Int8Array::from(vec![Some(-2), Some(7), None, Some(7)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(a)]).unwrap();
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["c"],
            scalar_fn(1, vec![col_ref(0), i8_literal(7)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn int16_column_gt_filter() {
        let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int16, true)]));
        let a = Int16Array::from(vec![Some(100), Some(200), Some(300)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(a)]).unwrap();
        let bytes = extended(
            &[(1, "gt:any_any")],
            &["c"],
            scalar_fn(1, vec![col_ref(0), i16_literal(150)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let mask = pf.evaluate(&batch).unwrap();
        assert!(!mask.value(0));
        assert!(mask.value(1));
        assert!(mask.value(2));
    }

    #[test]
    fn int32_column_lt_filter() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let a = Int32Array::from(vec![Some(1), Some(5), Some(9), Some(13)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(a)]).unwrap();
        let bytes = extended(
            &[(1, "lt:any_any")],
            &["id"],
            scalar_fn(1, vec![col_ref(0), i32_literal(9)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn float32_column_gte_filter() {
        let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Float32, true)]));
        let a = Float32Array::from(vec![Some(-1.5_f32), Some(0.0), Some(3.5)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(a)]).unwrap();
        let bytes = extended(
            &[(1, "gte:any_any")],
            &["c"],
            scalar_fn(1, vec![col_ref(0), f32_literal(0.0_f32)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn float64_column_ne_filter() {
        let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Float64, true)]));
        let a = Float64Array::from(vec![Some(1.0_f64), Some(2.0), Some(1.0), None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(a)]).unwrap();
        let bytes = extended(
            &[(1, "not_equal:any_any")],
            &["c"],
            scalar_fn(1, vec![col_ref(0), f64_literal(1.0_f64)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        // 1.0 → false, 2.0 → true, 1.0 → false, NULL → NULL (drop)
        assert_eq!(out.num_rows(), 1);
    }

    #[test]
    fn bool_column_equality_filter() {
        let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Boolean, true)]));
        let a = ArrBoolArr::from(vec![Some(true), Some(false), None, Some(true)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(a)]).unwrap();
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["c"],
            scalar_fn(1, vec![col_ref(0), bool_literal(true)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    // ─── Decimal (the case that the live test exposed) ────────────────────

    #[test]
    fn decimal128_column_equality_filter_same_scale() {
        // col_decimal DECIMAL(10,2) = -10.00  →  value=-1000 (scale 2)
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Decimal128(10, 2),
            true,
        )]));
        let arr: Decimal128Array = Decimal128Array::from(vec![
            Some(-1000_i128), // -10.00
            Some(1500_i128),  // 15.00
            None,
            Some(-1000_i128), // -10.00
        ])
        .with_precision_and_scale(10, 2)
        .unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["v"],
            scalar_fn(1, vec![col_ref(0), decimal_literal(-1000_i128, 10, 2)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn decimal128_column_gt_with_scale_rescale() {
        // Column has scale 4, scalar has scale 2 → rescale scalar up by 10^2.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Decimal128(18, 4),
            true,
        )]));
        let arr: Decimal128Array = Decimal128Array::from(vec![
            Some(500_000_i128),   // 50.0000
            Some(1_000_000_i128), // 100.0000
            Some(1_500_000_i128), // 150.0000
        ])
        .with_precision_and_scale(18, 4)
        .unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        // scalar: 75.00 in DECIMAL(5,2) → value = 7500, scale = 2.
        // Rescaled to col scale 4: 7500 * 10^2 = 750_000.
        let bytes = extended(
            &[(1, "gt:any_any")],
            &["v"],
            scalar_fn(1, vec![col_ref(0), decimal_literal(7500_i128, 5, 2)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let mask = pf.evaluate(&batch).unwrap();
        assert!(!mask.value(0)); // 50 > 75 false
        assert!(mask.value(1)); // 100 > 75 true
        assert!(mask.value(2)); // 150 > 75 true
    }

    // ─── Date / Timestamp ────────────────────────────────────────────────

    #[test]
    fn date32_column_filter() {
        // Days since 1970-01-01.  2000-01-01 = 10957, 2024-01-01 = 19723.
        let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, true)]));
        let arr = Date32Array::from(vec![Some(10957), Some(19723), Some(20000)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let bytes = extended(
            &[(1, "gte:any_any")],
            &["d"],
            scalar_fn(1, vec![col_ref(0), date_literal(19723)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn timestamp_micros_column_lt_filter() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));
        // µs since epoch
        let arr = TimestampMicrosecondArray::from(vec![
            Some(1_000_000_000_000_000_i64), // ~2001-09-09
            Some(1_700_000_000_000_000_i64), // ~2023-11
            Some(1_800_000_000_000_000_i64),
        ]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let bytes = extended(
            &[(1, "lt:any_any")],
            &["t"],
            scalar_fn(
                1,
                vec![col_ref(0), ts_micros_literal(1_500_000_000_000_000_i64)],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 1);
    }

    #[test]
    fn precision_timestamp_ms_rescales_to_us_column() {
        // PrecisionTimestamp{value=1_000_000_000_000, precision=3}  (ms)
        // = 1_000_000_000_000_000 µs (1e15).
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));
        let arr = TimestampMicrosecondArray::from(vec![
            Some(500_000_000_000_000_i64),   // ~half of scalar
            Some(2_000_000_000_000_000_i64), // ~twice
        ]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let bytes = extended(
            &[(1, "gte:any_any")],
            &["t"],
            scalar_fn(
                1,
                vec![col_ref(0), precision_ts_literal(1_000_000_000_000_i64, 3)],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let mask = pf.evaluate(&batch).unwrap();
        assert!(!mask.value(0));
        assert!(mask.value(1));
    }

    // ─── Binary / VarChar ────────────────────────────────────────────────

    #[test]
    fn binary_column_equality_filter() {
        let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Binary, true)]));
        let arr =
            BinaryArray::from_vec(vec![b"alpha".as_ref(), b"beta".as_ref(), b"gamma".as_ref()]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["b"],
            scalar_fn(1, vec![col_ref(0), binary_literal(b"beta")]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 1);
    }

    #[test]
    fn binary_column_gt_filter_lex_order() {
        // bytes order: "alpha" < "beta" < "gamma". gt "alpha" matches 2 rows.
        let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Binary, true)]));
        let arr =
            BinaryArray::from_vec(vec![b"alpha".as_ref(), b"beta".as_ref(), b"gamma".as_ref()]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let bytes = extended(
            &[(1, "gt:any_any")],
            &["b"],
            scalar_fn(1, vec![col_ref(0), binary_literal(b"alpha")]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn varchar_literal_matches_string_column() {
        let batch = make_batch_with_string();
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["s"],
            scalar_fn(1, vec![col_ref(0), varchar_literal("cherry")]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 1);
    }

    // ─── IS NOT NULL ─────────────────────────────────────────────────────

    #[test]
    fn is_not_null_filter() {
        let batch = make_batch_two_int_cols();
        // a IS NOT NULL  →  drops row 3
        let bytes = extended(
            &[(1, "is_not_null:any")],
            &["a", "b"],
            scalar_fn(1, vec![col_ref(0)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 4);
    }

    // ─── Mixed-type compound predicate (worst-case: multiple types in one
    //    Substrait expression, exercises all decode + dispatch paths) ──────

    #[test]
    fn mixed_type_compound_predicate() {
        // Schema: id Int32, name Utf8, amount Decimal(10,2), ts Timestamp(µs)
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(10, 2), true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ]));
        let id = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
        let name = StringArray::from(vec![
            Some("alice"),
            Some("bob"),
            Some("charlie"),
            Some("alice"),
        ]);
        let amount = Decimal128Array::from(vec![
            Some(1000_i128),
            Some(2000_i128),
            Some(500_i128),
            Some(2500_i128),
        ])
        .with_precision_and_scale(10, 2)
        .unwrap();
        let ts = TimestampMicrosecondArray::from(vec![
            Some(1_000_000_i64),
            Some(2_000_000_i64),
            Some(3_000_000_i64),
            Some(4_000_000_i64),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(id), Arc::new(name), Arc::new(amount), Arc::new(ts)],
        )
        .unwrap();

        // (name = 'alice' AND amount > 15.00) OR ts >= 4_000_000µs
        //   row 0: name=alice, amount=10.00 → 10>15 false. ts=1M >= 4M false. DROP
        //   row 1: name=bob → 'bob'='alice' false. ts=2M false. DROP
        //   row 2: name=charlie → false. ts=3M false. DROP
        //   row 3: name=alice, amount=25.00 → 25>15 true. KEEP
        let bytes = extended(
            &[
                (1, "or:bool_bool"),
                (2, "and:bool_bool"),
                (3, "equal:any_any"),
                (4, "gt:any_any"),
                (5, "gte:any_any"),
            ],
            &["id", "name", "amount", "ts"],
            scalar_fn(
                1,
                vec![
                    scalar_fn(
                        2,
                        vec![
                            scalar_fn(3, vec![col_ref(1), varchar_literal("alice")]),
                            scalar_fn(4, vec![col_ref(2), decimal_literal(1500_i128, 10, 2)]),
                        ],
                    ),
                    scalar_fn(5, vec![col_ref(3), ts_micros_literal(4_000_000_i64)]),
                ],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 1);
        let kept_id = out
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0);
        assert_eq!(kept_id, 4);
    }

    // ─── Literal-rescale regressions (0610 review) ────────────────────────

    #[test]
    fn decimal_literal_finer_scale_than_column_is_not_truncated() {
        // Column Decimal(10,0) holding 0 and 1; literal 0.5 = Decimal128{5,scale 1}.
        // `v = 0.5` must match NOTHING — an integer-scale column can't equal 0.5.
        // The old `sval / div` (5/10=0) silently turned this into `v = 0`.
        let arr = Decimal128Array::from(vec![Some(0_i128), Some(1_i128)])
            .with_precision_and_scale(10, 0)
            .unwrap();
        let col: ArrayRef = Arc::new(arr);
        let half = ScalarValue::Decimal128 {
            value: 5,
            precision: 2,
            scale: 1,
        };
        let out = compare_decimal_column(&col, Cmp::Eq, &half, 10, 0, false, 2).unwrap();
        assert!(!out.value(0), "0 must not equal 0.5");
        assert!(!out.value(1), "1 must not equal 0.5");
        // `v > 0.5` keeps only v = 1.
        let out = compare_decimal_column(&col, Cmp::Gt, &half, 10, 0, false, 2).unwrap();
        assert!(!out.value(0));
        assert!(out.value(1));
    }

    #[test]
    fn timestamp_nanosecond_literal_is_not_truncated() {
        // rescale to ns is lossless for ns precision.
        assert_eq!(rescale_to_nanos(1_000_000_001, 9).unwrap(), 1_000_000_001);
        let col: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![Some(
            1_000_000_001_i64,
        )]));
        // Exact ns literal matches; the µs-truncated value (…000) does not — the
        // old µs canonical form would have made both compare equal.
        let exact = ScalarValue::TimestampNanos(1_000_000_001);
        let out = compare_timestamp_column(&col, Cmp::Eq, &exact, TimeUnit::Nanosecond, false, 1)
            .unwrap();
        assert!(out.value(0));
        let truncated = ScalarValue::TimestampNanos(1_000_000_000);
        let out =
            compare_timestamp_column(&col, Cmp::Eq, &truncated, TimeUnit::Nanosecond, false, 1)
                .unwrap();
        assert!(!out.value(0));
    }

    #[test]
    fn timestamp_subsecond_literal_against_second_column_is_not_truncated() {
        // Column is seconds [1]; literal 1.5s = 1_500_000_000 ns. `ts >= 1.5s`
        // is false for ts = 1s. The old code divided the µs literal down to 1s,
        // making `ts >= 1` wrongly true.
        let col: ArrayRef = Arc::new(TimestampSecondArray::from(vec![Some(1_i64)]));
        let one_point_five = ScalarValue::TimestampNanos(1_500_000_000);
        let out =
            compare_timestamp_column(&col, Cmp::Ge, &one_point_five, TimeUnit::Second, false, 1)
                .unwrap();
        assert!(!out.value(0), "1s is not >= 1.5s");
    }

    #[test]
    fn timestamp_column_compares_in_nanoseconds_for_every_time_unit() {
        // The literal is canonical ns; the COLUMN is scaled up to ns. Pin that
        // for all four arrow TimeUnits, including the sub-unit literal case
        // where the boundary must not collapse onto the column's resolution.
        for (unit, ns_per_unit) in [
            (TimeUnit::Second, 1_000_000_000_i64),
            (TimeUnit::Millisecond, 1_000_000_i64),
            (TimeUnit::Microsecond, 1_000_i64),
            (TimeUnit::Nanosecond, 1_i64),
        ] {
            let values = vec![Some(1_i64), Some(2_i64), None, Some(3_i64)];
            let col: ArrayRef = match unit {
                TimeUnit::Second => Arc::new(TimestampSecondArray::from(values)),
                TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(values)),
                TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(values)),
                TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(values)),
            };

            let two = ScalarValue::TimestampNanos(2 * ns_per_unit);
            let mask = compare_column_scalar(&col, Cmp::Eq, &two, false, col.len())
                .unwrap_or_else(|e| panic!("{unit:?} eq: {e}"));
            assert!(!mask.value(0), "{unit:?}: 1 != 2");
            assert!(mask.value(1), "{unit:?}: 2 == 2");
            assert!(mask.is_null(2), "{unit:?}: null row stays null");
            assert!(!mask.value(3), "{unit:?}: 3 != 2");

            let mask = compare_column_scalar(&col, Cmp::Lt, &two, false, col.len())
                .unwrap_or_else(|e| panic!("{unit:?} lt: {e}"));
            assert!(mask.value(0), "{unit:?}: 1 < 2");
            assert!(!mask.value(1), "{unit:?}: 2 is not < 2");
            assert!(mask.is_null(2), "{unit:?}: null row stays null");

            // Reversed operand order: literal on the LHS flips the comparator.
            let mask = compare_column_scalar(&col, Cmp::Lt, &two, true, col.len())
                .unwrap_or_else(|e| panic!("{unit:?} reversed lt: {e}"));
            assert!(!mask.value(0), "{unit:?}: 2 is not < 1");
            assert!(mask.value(3), "{unit:?}: 2 < 3");

            if ns_per_unit == 1 {
                continue;
            }
            // Literal 2.5 units: not representable in the column's unit. `>= 2.5`
            // must exclude 2 (the old lossy literal-down rescale made it match).
            let two_and_a_half = ScalarValue::TimestampNanos(2 * ns_per_unit + ns_per_unit / 2);
            let mask = compare_column_scalar(&col, Cmp::Ge, &two_and_a_half, false, col.len())
                .unwrap_or_else(|e| panic!("{unit:?} gte: {e}"));
            assert!(!mask.value(1), "{unit:?}: 2 is not >= 2.5");
            assert!(mask.value(3), "{unit:?}: 3 >= 2.5");
            let mask = compare_column_scalar(&col, Cmp::Eq, &two_and_a_half, false, col.len())
                .unwrap_or_else(|e| panic!("{unit:?} eq subunit: {e}"));
            assert!(
                (0..mask.len()).all(|i| mask.is_null(i) || !mask.value(i)),
                "{unit:?}: no whole-unit value can equal 2.5"
            );
        }
    }

    #[test]
    fn timestamp_column_type_mismatch_is_error() {
        // A non-timestamp literal against a timestamp column must Err (routing
        // to the safe fallbacks), never panic.
        let col: ArrayRef = Arc::new(TimestampSecondArray::from(vec![Some(1_i64)]));
        let err = compare_column_scalar(&col, Cmp::Eq, &ScalarValue::I64(1), false, 1).unwrap_err();
        assert!(err.contains("type mismatch"), "got: {err}");
    }

    // ════════════════════════════════════════════════════════════════════════
    // SQL IN — substrait `SingularOrList`
    // ════════════════════════════════════════════════════════════════════════

    fn in_list(value: Expression, options: Vec<Expression>) -> Expression {
        Expression {
            rex_type: Some(RexType::SingularOrList(Box::new(SingularOrList {
                value: Some(Box::new(value)),
                options,
            }))),
        }
    }

    /// A NULL literal typed as Utf8, for the string IN test.
    fn null_string_literal() -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: true,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::Null(Type {
                    kind: Some(r#type::Kind::String(r#type::String::default())),
                })),
            })),
        }
    }

    #[test]
    fn in_list_decodes_and_evaluates_over_int_column() {
        let batch = make_batch_two_int_cols();
        // a IN (2, 5) — no substrait function anchors are involved at all.
        let bytes = extended(
            &[],
            &["a", "b"],
            in_list(col_ref(0), vec![i64_literal(2), i64_literal(5)]),
        );
        let pf = PushedFilter::decode(&bytes)
            .expect("IN must decode")
            .expect("IN must not drop the filter");

        let mask = pf.evaluate(&batch).unwrap();
        assert_eq!(mask.len(), 5);
        assert!(!mask.value(0), "a=1 not in (2, 5)");
        assert!(mask.value(1), "a=2 in (2, 5)");
        assert!(!mask.value(2), "a=3 not in (2, 5)");
        assert!(mask.is_null(3), "a=NULL yields NULL, not false");
        assert!(mask.value(4), "a=5 in (2, 5)");

        let filtered = filter_batch(&batch, &pf).unwrap();
        let a = filtered
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(a.values(), &[2, 5]);
    }

    #[test]
    fn in_list_with_null_option_is_three_valued() {
        let batch = make_batch_two_int_cols();
        // a IN (2, NULL): a definite match is TRUE; every non-match is NULL
        // (and therefore dropped), never FALSE.
        let bytes = extended(
            &[],
            &["a", "b"],
            in_list(col_ref(0), vec![i64_literal(2), null_literal()]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let mask = pf.evaluate(&batch).unwrap();
        assert!(
            mask.is_null(0),
            "a=1 matches nothing but NULL is in the list"
        );
        assert!(mask.value(1), "a=2 matches definitely, despite the NULL");
        assert!(mask.is_null(2), "a=3 matches nothing → NULL");
        assert!(mask.is_null(3), "a=NULL → NULL");
        assert!(mask.is_null(4), "a=5 matches nothing → NULL");

        let filtered = filter_batch(&batch, &pf).unwrap();
        let a = filtered
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(a.values(), &[2]);
    }

    #[test]
    fn in_list_over_string_column() {
        let batch = make_batch_with_string();
        // s IN ('apple', 'cherry') over [apple, banana, NULL, cherry]
        let bytes = extended(
            &[],
            &["s"],
            in_list(
                col_ref(0),
                vec![string_literal("apple"), varchar_literal("cherry")],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let mask = pf.evaluate(&batch).unwrap();
        assert!(mask.value(0));
        assert!(!mask.value(1));
        assert!(mask.is_null(2));
        assert!(mask.value(3));

        let filtered = filter_batch(&batch, &pf).unwrap();
        let s = filtered
            .column_by_name("s")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(s.value(0), "apple");
        assert_eq!(s.value(1), "cherry");
        assert_eq!(s.len(), 2);

        // A NULL option keeps the same three-valued behaviour for strings.
        let bytes = extended(
            &[],
            &["s"],
            in_list(
                col_ref(0),
                vec![string_literal("apple"), null_string_literal()],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let mask = pf.evaluate(&batch).unwrap();
        assert!(mask.value(0), "apple matches definitely");
        assert!(mask.is_null(1), "banana matches nothing → NULL");
    }

    #[test]
    fn in_list_over_non_integer_columns() {
        // Float64 and Date32 pin the scalar-array building path for column types
        // that aren't plain integers.
        let schema = Arc::new(Schema::new(vec![
            Field::new("f", DataType::Float64, true),
            Field::new("d", DataType::Date32, true),
        ]));
        let f = Float64Array::from(vec![Some(1.5_f64), Some(2.5), None, Some(3.5)]);
        let d = Date32Array::from(vec![Some(10957), Some(19723), Some(20000), None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(f), Arc::new(d)]).unwrap();

        let bytes = extended(
            &[],
            &["f", "d"],
            in_list(col_ref(0), vec![f64_literal(1.5), f64_literal(3.5)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let mask = pf.evaluate(&batch).unwrap();
        assert!(mask.value(0));
        assert!(!mask.value(1));
        assert!(mask.is_null(2));
        assert!(mask.value(3));

        let bytes = extended(
            &[],
            &["f", "d"],
            in_list(col_ref(1), vec![date_literal(19723), date_literal(20000)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let mask = pf.evaluate(&batch).unwrap();
        assert!(!mask.value(0));
        assert!(mask.value(1));
        assert!(mask.value(2));
        assert!(mask.is_null(3));
    }

    #[test]
    fn not_in_list_inverts_and_preserves_kleene_null() {
        let batch = make_batch_two_int_cols();
        // NOT (a IN (2, 5)) — plain inversion, NULL row stays NULL.
        let bytes = extended(
            &[(1, "not:bool")],
            &["a", "b"],
            scalar_fn(
                1,
                vec![in_list(col_ref(0), vec![i64_literal(2), i64_literal(5)])],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let mask = pf.evaluate(&batch).unwrap();
        assert!(mask.value(0));
        assert!(!mask.value(1));
        assert!(mask.value(2));
        assert!(mask.is_null(3));
        assert!(!mask.value(4));
        let filtered = filter_batch(&batch, &pf).unwrap();
        let a = filtered
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(a.values(), &[1, 3]);

        // NOT (a IN (2, NULL)) — a non-matching row is NULL inside the IN, and
        // Kleene NOT leaves it NULL rather than flipping it to TRUE. Every row
        // is therefore dropped, matching SQL's `NOT IN` with a NULL in the list.
        let bytes = extended(
            &[(1, "not:bool")],
            &["a", "b"],
            scalar_fn(
                1,
                vec![in_list(col_ref(0), vec![i64_literal(2), null_literal()])],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let mask = pf.evaluate(&batch).unwrap();
        assert!(mask.is_null(0), "non-matching row stays NULL after NOT");
        assert!(!mask.value(1), "a=2 was a definite match → NOT is FALSE");
        assert!(mask.is_null(2));
        assert!(mask.is_null(3));
        assert!(mask.is_null(4));
        assert_eq!(filter_batch(&batch, &pf).unwrap().num_rows(), 0);
    }

    #[test]
    fn in_list_pk_gating_follows_the_referenced_column() {
        // IN over the PK column → safe for MOR pushdown.
        let bytes = extended(
            &[],
            &["a", "b"],
            in_list(col_ref(0), vec![i64_literal(2), i64_literal(5)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert_eq!(
            pf.referenced_field_indices()
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![0_usize]
        );
        assert!(pf.references_only_primary_keys(&["a".to_string()]));

        // IN over a non-PK column → not safe.
        let bytes = extended(&[], &["a", "b"], in_list(col_ref(1), vec![i64_literal(20)]));
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert_eq!(
            pf.referenced_field_indices()
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![1_usize]
        );
        assert!(!pf.references_only_primary_keys(&["a".to_string()]));
    }

    #[test]
    fn in_list_only_predicate_installs_row_filter() {
        // IN is equality-like, so parquet stats can prune on it: an IN-only
        // predicate must pass the ENG-42276 v4.1 selectivity gate.
        let bytes = extended(
            &[],
            &["a", "b"],
            in_list(col_ref(0), vec![i64_literal(2), i64_literal(5)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let schema = make_parquet_schema(&["a", "b"]);
        assert!(
            pf.build_row_filter(&schema).is_some(),
            "an IN-only predicate must install a parquet RowFilter"
        );
    }

    #[test]
    fn in_list_with_non_literal_option_is_error() {
        // `a IN (b)` — a column option isn't something we evaluate; Err routes
        // to the safe fallbacks rather than silently mis-filtering.
        let batch = make_batch_two_int_cols();
        let bytes = extended(&[], &["a", "b"], in_list(col_ref(0), vec![col_ref(1)]));
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let err = pf.evaluate(&batch).unwrap_err();
        assert!(err.contains("option is not a literal"), "got: {err}");
    }

    #[test]
    fn in_list_with_empty_options_is_error() {
        // An empty IN list would fold to an all-false mask, which at the parquet
        // RowFilter prunes every row unrecoverably. Must Err instead.
        let batch = make_batch_two_int_cols();
        let bytes = extended(&[], &["a", "b"], in_list(col_ref(0), vec![]));
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let err = pf.evaluate(&batch).unwrap_err();
        assert!(err.contains("no options"), "got: {err}");
    }

    #[test]
    fn in_list_over_scalar_value_is_error() {
        // The tested value must be a column, not a constant.
        let batch = make_batch_two_int_cols();
        let bytes = extended(
            &[],
            &["a", "b"],
            in_list(i64_literal(1), vec![i64_literal(2)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let err = pf.evaluate(&batch).unwrap_err();
        assert!(err.contains("must be a column"), "got: {err}");
    }

    // ─── Decode-time soft-fail: unsupported literal types ─────────────────

    #[test]
    fn list_literal_is_currently_unsupported() {
        // List literal isn't in our supported set; decode_literal should
        // return Err (caller wraps that into a hard error today). This
        // documents the behaviour so a future "drop predicate on unsupported
        // literal" change is visible in the test diff.
        use substrait::proto::expression::literal::List as LiteralList;
        let lit_list = LiteralType::List(LiteralList { values: vec![] });
        let lit = Literal {
            nullable: false,
            type_variation_reference: 0,
            literal_type: Some(lit_list),
        };
        let err = decode_literal(&lit).unwrap_err();
        assert!(err.contains("unsupported literal type"), "got: {err}");
    }

    // ─── Coverage marker: every ScalarValue variant has a constructor test
    //    above. If you add a new variant, also add a test here. ─────────────

    #[test]
    fn scalar_value_coverage_marker() {
        // This isn't a behaviour test — it's a checklist. Compilation alone
        // proves that each variant constructor still exists; the tests above
        // exercise the evaluator path for each.
        let _ = ScalarValue::Bool(false);
        let _ = ScalarValue::I8(0);
        let _ = ScalarValue::I16(0);
        let _ = ScalarValue::I32(0);
        let _ = ScalarValue::I64(0);
        let _ = ScalarValue::F32(0.0);
        let _ = ScalarValue::F64(0.0);
        let _ = ScalarValue::String(String::new());
        let _ = ScalarValue::Binary(vec![]);
        let _ = ScalarValue::Date(0);
        let _ = ScalarValue::TimestampNanos(0);
        let _ = ScalarValue::Decimal128 {
            value: 0,
            precision: 1,
            scale: 0,
        };
        let _ = ScalarValue::Null;
    }

    // Suppress unused-import warning for ptype if tests don't use it.
    #[allow(dead_code)]
    fn _ptype_anchor() -> ptype::Struct {
        ptype::Struct::default()
    }

    // ════════════════════════════════════════════════════════════════════
    // ENG-42276 — Parquet RowFilter pushdown tests
    // ════════════════════════════════════════════════════════════════════
    //
    // These tests verify the parquet-level pushdown adapter on `PushedFilter`:
    //
    //   - `referenced_field_indices` walks the substrait expression tree and
    //     returns exactly the field indices read by `Selection`s.
    //   - `build_row_filter` constructs a `RowFilter` when every referenced
    //     column exists in the parquet file's top-level schema, returning
    //     `None` (graceful skip — post-merge filter still runs) when any
    //     referenced column is missing or the expression is degenerate.
    //   - The `ArrowPredicate` wrapper produces the same mask as the
    //     underlying `PushedFilter::evaluate` on a matching batch, and
    //     gracefully returns an all-true mask (rather than failing the
    //     parquet read) when underlying evaluation errors.
    //
    // Together these cover all the safety/correctness invariants needed by
    // the FFI gate in `lib.rs` and the COW-vs-MOR gate in
    // `file_group/reader/mod.rs`.

    use parquet::arrow::arrow_reader::ArrowPredicate;
    use parquet::basic::Type as ParquetPhysicalType;
    use parquet::schema::types::{SchemaDescriptor, Type as ParquetType};

    /// Build a parquet `SchemaDescriptor` whose root has primitive INT64
    /// columns named by `field_names`. Sufficient for tests of name-based
    /// column resolution; per-column physical type doesn't matter because
    /// `build_row_filter` only looks at names.
    fn make_parquet_schema(field_names: &[&str]) -> SchemaDescriptor {
        let fields: Vec<std::sync::Arc<ParquetType>> = field_names
            .iter()
            .map(|n| {
                std::sync::Arc::new(
                    ParquetType::primitive_type_builder(n, ParquetPhysicalType::INT64)
                        .build()
                        .unwrap(),
                )
            })
            .collect();
        let root = ParquetType::group_type_builder("schema")
            .with_fields(fields)
            .build()
            .unwrap();
        SchemaDescriptor::new(std::sync::Arc::new(root))
    }

    #[test]
    fn pushdown_referenced_fields_single_selection() {
        // Expression: a == 5. Should reference only field index 0.
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["a", "b"],
            scalar_fn(1, vec![col_ref(0), i64_literal(5)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let refs = pf.referenced_field_indices();
        assert_eq!(refs.iter().copied().collect::<Vec<_>>(), vec![0_usize]);
    }

    #[test]
    fn pushdown_referenced_fields_two_selections_across_and() {
        // Expression: (a > 1) AND (b < 10). Should reference indices 0 and 1.
        let bytes = extended(
            &[(1, "gt:any_any"), (2, "lt:any_any"), (3, "and:bool_bool")],
            &["a", "b"],
            scalar_fn(
                3,
                vec![
                    scalar_fn(1, vec![col_ref(0), i64_literal(1)]),
                    scalar_fn(2, vec![col_ref(1), i64_literal(10)]),
                ],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let refs = pf.referenced_field_indices();
        assert_eq!(refs.iter().copied().collect::<Vec<_>>(), vec![0_usize, 1]);
    }

    #[test]
    fn pushdown_referenced_fields_dedups_same_column() {
        // Expression: (a > 1) AND (a < 10). Both branches reference column a.
        // referenced_field_indices uses BTreeSet → deduped result.
        let bytes = extended(
            &[(1, "gt:any_any"), (2, "lt:any_any"), (3, "and:bool_bool")],
            &["a", "b"],
            scalar_fn(
                3,
                vec![
                    scalar_fn(1, vec![col_ref(0), i64_literal(1)]),
                    scalar_fn(2, vec![col_ref(0), i64_literal(10)]),
                ],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let refs = pf.referenced_field_indices();
        assert_eq!(refs.iter().copied().collect::<Vec<_>>(), vec![0_usize]);
    }

    // ════════════════════════════════════════════════════════════════════
    // referenced_columns — the predicate-scoped counterpart to columns()
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn referenced_columns_is_a_strict_subset_of_the_base_schema() {
        // Gluten serialises the whole base schema into the blob, so `columns()`
        // names `b` even though the expression never reads it.
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["a", "b"],
            scalar_fn(1, vec![col_ref(0), i64_literal(5)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert_eq!(pf.columns(), ["a", "b"], "base schema, verbatim");
        assert_eq!(
            pf.referenced_columns(),
            vec!["a".to_string()],
            "only the column the expression reads"
        );
    }

    #[test]
    fn referenced_columns_returns_every_referenced_column_deduped() {
        // (a > 1) AND (b < 10) AND (a < 100): both columns, `a` once.
        let bytes = extended(
            &[
                (1, "gt:any_any"),
                (2, "lt:any_any"),
                (3, "and:bool_bool"),
                (4, "lt:any_any"),
            ],
            &["a", "b", "c"],
            scalar_fn(
                3,
                vec![
                    scalar_fn(1, vec![col_ref(0), i64_literal(1)]),
                    scalar_fn(
                        3,
                        vec![
                            scalar_fn(2, vec![col_ref(1), i64_literal(10)]),
                            scalar_fn(4, vec![col_ref(0), i64_literal(100)]),
                        ],
                    ),
                ],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert_eq!(
            pf.referenced_columns(),
            vec!["a".to_string(), "b".to_string()],
            "deduped, in field-index order, and `c` is never referenced"
        );
    }

    #[test]
    fn referenced_columns_skips_an_index_out_of_range_of_the_base_schema() {
        // A malformed plan referencing field 5 of a 1-column base schema. Skipping
        // suits the repair pre-screen this feeds; the PK gate must not guess, and
        // rejects the same shape outright.
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["a"],
            scalar_fn(1, vec![col_ref(5), i64_literal(5)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert!(pf.referenced_columns().is_empty());
        assert!(
            !pf.references_only_primary_keys(&["a".to_string()]),
            "the PK gate still refuses a plan it cannot resolve"
        );
    }

    #[test]
    fn pushdown_build_row_filter_some_when_column_present() {
        // Expression: a == 5. Parquet schema has [a, b]. Should produce a
        // RowFilter (not None).
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["a", "b"],
            scalar_fn(1, vec![col_ref(0), i64_literal(5)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let schema = make_parquet_schema(&["a", "b"]);
        let rf = pf.build_row_filter(&schema);
        assert!(rf.is_some(), "should build a RowFilter when column exists");
    }

    // ════════════════════════════════════════════════════════════════════
    // references_only_primary_keys — ENG-42866 PK-safe MOR pushdown gate
    // ════════════════════════════════════════════════════════════════════
    //
    // Mirrors Java's filterIsSafeForPrimaryKey
    // (SparkFileFormatInternalRowReaderContext.scala:285-289). Test matrix:
    //
    //   PK fields       | predicate refs              | expected
    //   ----------------+-----------------------------+----------
    //   ["id"]          | a == 5                      | false      (non-PK col)
    //   ["id"]          | id == 5                     | true       (PK col)
    //   ["id"]          | id == 5 AND id < 100        | true       (PK twice)
    //   ["id"]          | id == 5 AND a < 100         | false      (PK + non-PK)
    //   ["id", "ts"]    | id == 5 AND ts < 100        | true       (composite key)
    //   []              | _hoodie_record_key == 'a'   | true       (meta col always allowed)
    //   ["ID"]          | id == 5                     | true       (case-insensitive)
    //   ["id"]          | <constant predicate>        | true       (vacuous — no refs)
    //   []              | a == 5                      | false      (no PKs configured, non-meta ref)

    #[test]
    fn pushdown_pk_safe_non_pk_column_returns_false() {
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["a", "b"],
            scalar_fn(1, vec![col_ref(0), i64_literal(5)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert!(!pf.references_only_primary_keys(&["id".to_string()]));
    }

    #[test]
    fn pushdown_pk_safe_pk_column_returns_true() {
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["id", "data"],
            scalar_fn(1, vec![col_ref(0), i64_literal(5)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert!(pf.references_only_primary_keys(&["id".to_string()]));
    }

    #[test]
    fn pushdown_pk_safe_same_pk_twice_returns_true() {
        // id == 5 AND id < 100 — same PK column referenced twice.
        let bytes = extended(
            &[
                (1, "equal:any_any"),
                (2, "lt:any_any"),
                (3, "and:bool_bool"),
            ],
            &["id"],
            scalar_fn(
                3,
                vec![
                    scalar_fn(1, vec![col_ref(0), i64_literal(5)]),
                    scalar_fn(2, vec![col_ref(0), i64_literal(100)]),
                ],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert!(pf.references_only_primary_keys(&["id".to_string()]));
    }

    #[test]
    fn pushdown_pk_safe_mixed_pk_and_non_pk_returns_false() {
        // id == 5 AND a < 100 — PK + non-PK — Java's gate rejects.
        let bytes = extended(
            &[
                (1, "equal:any_any"),
                (2, "lt:any_any"),
                (3, "and:bool_bool"),
            ],
            &["id", "a"],
            scalar_fn(
                3,
                vec![
                    scalar_fn(1, vec![col_ref(0), i64_literal(5)]),
                    scalar_fn(2, vec![col_ref(1), i64_literal(100)]),
                ],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert!(!pf.references_only_primary_keys(&["id".to_string()]));
    }

    #[test]
    fn pushdown_pk_safe_composite_key_returns_true() {
        // Composite PK (id, ts). Predicate references both — safe.
        let bytes = extended(
            &[
                (1, "equal:any_any"),
                (2, "lt:any_any"),
                (3, "and:bool_bool"),
            ],
            &["id", "ts"],
            scalar_fn(
                3,
                vec![
                    scalar_fn(1, vec![col_ref(0), i64_literal(5)]),
                    scalar_fn(2, vec![col_ref(1), i64_literal(100)]),
                ],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert!(pf.references_only_primary_keys(&["id".to_string(), "ts".to_string()]));
    }

    #[test]
    fn pushdown_pk_safe_hoodie_record_key_meta_column_returns_true() {
        // No PK fields configured but predicate uses the _hoodie_record_key
        // meta column — Java's gate always allows this branch via the
        // explicit equalsIgnoreCase check.
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["_hoodie_record_key"],
            scalar_fn(1, vec![col_ref(0), i64_literal(5)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert!(pf.references_only_primary_keys(&[]));
    }

    #[test]
    fn pushdown_pk_safe_case_insensitive() {
        // PK configured as "ID" (uppercase); predicate column is "id" (lower).
        // Java does .toLowerCase on both sides — our impl must too.
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["id"],
            scalar_fn(1, vec![col_ref(0), i64_literal(5)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert!(pf.references_only_primary_keys(&["ID".to_string()]));
    }

    #[test]
    fn pushdown_pk_safe_empty_predicate_returns_true() {
        // Predicate that references no columns (constant-only). forall is
        // vacuously true — same as Java's Filter.references.forall on []
        // returning true.
        //
        // We build this as a literal-only equal expression — the field
        // collector sees no Selection nodes and returns an empty set.
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["id"],
            scalar_fn(1, vec![i64_literal(1), i64_literal(1)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert!(pf.references_only_primary_keys(&["id".to_string()]));
    }

    #[test]
    fn pushdown_pk_safe_no_pks_non_meta_ref_returns_false() {
        // No PK fields configured at all, no meta column either. Predicate
        // references "a". Must return false — there's nothing to make this
        // safe under the morFilters rule.
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["a"],
            scalar_fn(1, vec![col_ref(0), i64_literal(5)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert!(!pf.references_only_primary_keys(&[]));
    }

    #[test]
    fn pushdown_build_row_filter_some_with_extra_parquet_cols() {
        // Predicate references only `a`. Parquet schema has [_hoodie_commit_time,
        // a, b, c] — the predicate column exists alongside Hudi metadata
        // columns and extras. Should still produce Some.
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["a"],
            scalar_fn(1, vec![col_ref(0), i64_literal(5)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let schema = make_parquet_schema(&["_hoodie_commit_time", "a", "b", "c"]);
        let rf = pf.build_row_filter(&schema);
        assert!(
            rf.is_some(),
            "extra parquet columns should not block pushdown"
        );
    }

    #[test]
    fn pushdown_build_row_filter_none_when_column_missing() {
        // Predicate references column 'z' but parquet schema only has [a, b].
        // Should return None — graceful skip; post-merge filter handles it.
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["z"],
            scalar_fn(1, vec![col_ref(0), i64_literal(5)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let schema = make_parquet_schema(&["a", "b"]);
        let rf = pf.build_row_filter(&schema);
        assert!(
            rf.is_none(),
            "pushdown must skip when referenced column not in parquet schema"
        );
    }

    #[test]
    fn pushdown_arrow_predicate_evaluate_matches_filter_evaluate() {
        // Sanity: the ArrowPredicate wrapping a PushedFilter produces the
        // same boolean mask as PushedFilter::evaluate on the same batch.
        let batch = make_batch_two_int_cols();
        let bytes = extended(
            &[(1, "lt:any_any")],
            &["a", "b"],
            scalar_fn(1, vec![col_ref(0), i64_literal(3)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();

        // Reference mask via the existing evaluator.
        let expected = pf.evaluate(&batch).unwrap();

        // Mask via the parquet ArrowPredicate adapter. ProjectionMask here
        // is irrelevant for the evaluator (it's just bookkeeping for parquet);
        // we use a no-op all-roots mask.
        let schema = make_parquet_schema(&["a", "b"]);
        let projection = ProjectionMask::roots(&schema, vec![0_usize, 1]);
        let mut pred = PushedFilterArrowPredicate {
            filter: pf,
            projection,
        };
        let got = pred.evaluate(batch).unwrap();
        assert_eq!(format!("{got:?}"), format!("{expected:?}"));
    }

    #[test]
    fn pushdown_v4_1_skips_pure_isnotnull_predicate() {
        // ENG-42276 v4.1 selectivity gate.
        // Predicate: isnotnull(col 0). All-isnotnull expressions can never
        // prune any row group via column stats (every row group with
        // any non-null row passes). build_row_filter must skip pushdown.
        let bytes = extended(
            &[(1, "is_not_null:any")],
            &["a", "b"],
            scalar_fn(1, vec![col_ref(0)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let schema = make_parquet_schema(&["a", "b"]);
        let rf = pf.build_row_filter(&schema);
        assert!(
            rf.is_none(),
            "pure IS NOT NULL must be skipped to avoid the wasted second decode pass"
        );
    }

    #[test]
    fn pushdown_v4_1_skips_isnotnull_conjunction() {
        // Predicate: isnotnull(a) AND isnotnull(b). Same logic as above:
        // an AND of two non-prunable predicates is itself non-prunable.
        let bytes = extended(
            &[
                (1, "is_not_null:any"),
                (2, "is_not_null:any"),
                (3, "and:bool_bool"),
            ],
            &["a", "b"],
            scalar_fn(
                3,
                vec![
                    scalar_fn(1, vec![col_ref(0)]),
                    scalar_fn(2, vec![col_ref(1)]),
                ],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let schema = make_parquet_schema(&["a", "b"]);
        let rf = pf.build_row_filter(&schema);
        assert!(
            rf.is_none(),
            "AND of pure IS NOT NULL conjuncts must be skipped"
        );
    }

    // ── ENG-47480: IsNull is selective; IsNotNull is not ──────────────────
    //
    // These four pin the asymmetry in `is_worth_row_filtering`. The gate was
    // originally justified as "column stats can't prune a null check", which
    // is true of both null checks and led to both being declined. The real
    // question is how many rows the predicate rejects, and by that measure the
    // two are opposites. Measured on TPC-DS 1TB q76, declining `IsNull` left
    // 4.83B rows to be re-filtered by Velox instead.

    // ── ENG-47483 R4 — OR is not AND. ────────────────────────────────────
    // An OR is only as selective as its least selective branch, so a
    // permissive leaf must veto the whole predicate. Judging an OR by its most
    // selective branch reintroduces the ENG-42276 v4.1 two-pass regression.

    #[test]
    fn or_of_selective_and_permissive_does_not_install_a_filter() {
        // `a IS NULL OR b IS NOT NULL`. The IsNull half is highly selective,
        // the IsNotNull half passes ~97% — so the union passes ~97% and the
        // second decode pass buys nothing.
        let bytes = extended(
            &[(1, "or:bool"), (2, "is_null:any"), (3, "is_not_null:any")],
            &["a", "b"],
            scalar_fn(
                1,
                vec![
                    scalar_fn(2, vec![col_ref(0)]),
                    scalar_fn(3, vec![col_ref(1)]),
                ],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert!(
            !pf.is_worth_row_filtering(&pf.expression, false),
            "an OR is only as selective as its weakest branch; the IsNotNull \
             branch alone passes almost everything"
        );
    }

    // ── ENG-47483: row-group pruning from footer statistics ───────────────
    //
    // The invariant under test is asymmetric. Keeping a group that cannot match
    // wastes time; dropping one that can match silently loses rows that nothing
    // downstream can restore. So every "keep" case below matters more than the
    // "drop" cases, and the defaults are all keep.

    fn rg_meta(rows: i64, col: &str, nulls: Option<u64>) -> RowGroupMetaData {
        use parquet::basic::Type as PT;
        use parquet::file::metadata::ColumnChunkMetaData;
        use parquet::file::statistics::Statistics;
        let field = std::sync::Arc::new(
            ParquetType::primitive_type_builder(col, PT::INT64)
                .build()
                .unwrap(),
        );
        let root = ParquetType::group_type_builder("schema")
            .with_fields(vec![field])
            .build()
            .unwrap();
        let descr = SchemaDescriptor::new(std::sync::Arc::new(root));
        let mut cc = ColumnChunkMetaData::builder(descr.column(0));
        if let Some(n) = nulls {
            cc = cc.set_statistics(Statistics::int64(None, None, None, Some(n), false));
        }
        RowGroupMetaData::builder(std::sync::Arc::new(descr))
            .set_num_rows(rows)
            .set_column_metadata(vec![cc.build().unwrap()])
            .build()
            .unwrap()
    }

    #[test]
    fn eng47483_isnull_drops_a_group_with_no_nulls() {
        let bytes = extended(
            &[(1, "is_null:any")],
            &["a"],
            scalar_fn(1, vec![col_ref(0)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert!(
            !pf.group_can_match(&pf.expression, &rg_meta(100, "a", Some(0))),
            "null_count == 0 proves IS NULL matches nothing here"
        );
        assert!(
            pf.group_can_match(&pf.expression, &rg_meta(100, "a", Some(1))),
            "one null is enough to keep the group"
        );
    }

    #[test]
    fn or_of_two_selective_branches_does_install_a_filter() {
        // `a = 1 OR b = 2` — both branches reject most rows, so the union
        // still does. The fix must not turn `all` into "never install".
        let bytes = extended(
            &[(1, "or:bool"), (2, "equal:any_any"), (3, "equal:any_any")],
            &["a", "b"],
            scalar_fn(
                1,
                vec![
                    scalar_fn(2, vec![col_ref(0), i64_literal(1)]),
                    scalar_fn(3, vec![col_ref(1), i64_literal(2)]),
                ],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert!(pf.is_worth_row_filtering(&pf.expression, false));
    }

    #[test]
    fn and_still_installs_when_only_one_branch_is_selective() {
        // `a IS NULL AND b IS NOT NULL` — an AND is as selective as its BEST
        // branch, so this must still install. Pins that the fix did not
        // collapse both combinators onto the `all` rule.
        let bytes = extended(
            &[(1, "and:bool"), (2, "is_null:any"), (3, "is_not_null:any")],
            &["a", "b"],
            scalar_fn(
                1,
                vec![
                    scalar_fn(2, vec![col_ref(0)]),
                    scalar_fn(3, vec![col_ref(1)]),
                ],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert!(pf.is_worth_row_filtering(&pf.expression, false));
    }

    #[test]
    fn negation_swaps_which_combinator_rule_applies() {
        // De Morgan: `NOT(a IS NOT NULL AND b IS NULL)` is
        // `a IS NULL OR b IS NOT NULL` — the permissive OR of the first test,
        // written as a negated AND. The effective-OR rule has to see through
        // the Not, or the same regression returns via the negated form.
        let bytes = extended(
            &[
                (1, "not:bool"),
                (2, "and:bool"),
                (3, "is_not_null:any"),
                (4, "is_null:any"),
            ],
            &["a", "b"],
            scalar_fn(
                1,
                vec![scalar_fn(
                    2,
                    vec![
                        scalar_fn(3, vec![col_ref(0)]),
                        scalar_fn(4, vec![col_ref(1)]),
                    ],
                )],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert!(
            !pf.is_worth_row_filtering(&pf.expression, false),
            "a negated AND is an effective OR and takes the `all` rule"
        );
    }

    #[test]
    fn eng47483_isnotnull_drops_an_all_null_group() {
        let bytes = extended(
            &[(1, "is_not_null:any")],
            &["a"],
            scalar_fn(1, vec![col_ref(0)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert!(
            !pf.group_can_match(&pf.expression, &rg_meta(100, "a", Some(100))),
            "every value null, so IS NOT NULL matches nothing"
        );
        assert!(
            pf.group_can_match(&pf.expression, &rg_meta(100, "a", Some(99))),
            "one non-null is enough to keep the group"
        );
    }

    #[test]
    fn eng47483_missing_statistics_keeps_the_group() {
        // The single most important case: absent stats must never be read as
        // "prunable". A file written without statistics would otherwise lose
        // every row.
        let bytes = extended(
            &[(1, "is_null:any")],
            &["a"],
            scalar_fn(1, vec![col_ref(0)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert!(pf.group_can_match(&pf.expression, &rg_meta(100, "a", None)));
    }

    #[test]
    fn eng47483_unresolvable_column_keeps_the_group() {
        // Predicate references column "a"; the group only has "other".
        let bytes = extended(
            &[(1, "is_null:any")],
            &["a"],
            scalar_fn(1, vec![col_ref(0)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert!(pf.group_can_match(&pf.expression, &rg_meta(100, "other", Some(0))));
    }

    #[test]
    fn eng47483_negation_keeps_the_group() {
        // NOT(IS NULL) is IS NOT NULL. If negation were ignored and the inner
        // verdict used directly, a group with no nulls would be dropped -- and
        // it is exactly the group that matches.
        let bytes = extended(
            &[(1, "is_null:any"), (2, "not")],
            &["a"],
            scalar_fn(2, vec![scalar_fn(1, vec![col_ref(0)])]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert!(pf.group_can_match(&pf.expression, &rg_meta(100, "a", Some(0))));
    }

    #[test]
    fn eng47483_and_drops_when_either_conjunct_is_impossible() {
        // isnull(a) AND isnotnull(a): unsatisfiable for a group with no nulls.
        let bytes = extended(
            &[
                (1, "is_null:any"),
                (2, "is_not_null:any"),
                (3, "and:bool_bool"),
            ],
            &["a"],
            scalar_fn(
                3,
                vec![
                    scalar_fn(1, vec![col_ref(0)]),
                    scalar_fn(2, vec![col_ref(0)]),
                ],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert!(!pf.group_can_match(&pf.expression, &rg_meta(100, "a", Some(0))));
        assert!(pf.group_can_match(&pf.expression, &rg_meta(100, "a", Some(5))));
    }

    #[test]
    fn eng47483_or_keeps_when_either_disjunct_is_possible() {
        let bytes = extended(
            &[
                (1, "is_null:any"),
                (2, "is_not_null:any"),
                (3, "or:bool_bool"),
            ],
            &["a"],
            scalar_fn(
                3,
                vec![
                    scalar_fn(1, vec![col_ref(0)]),
                    scalar_fn(2, vec![col_ref(0)]),
                ],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        // No nulls: the IS NULL side is impossible, the IS NOT NULL side is not.
        assert!(pf.group_can_match(&pf.expression, &rg_meta(100, "a", Some(0))));
    }

    fn rg_int_range(rows: i64, col: &str, lo: i64, hi: i64, nulls: u64) -> RowGroupMetaData {
        use parquet::basic::Type as PT;
        use parquet::file::metadata::ColumnChunkMetaData;
        use parquet::file::statistics::Statistics;
        let field = std::sync::Arc::new(
            ParquetType::primitive_type_builder(col, PT::INT64)
                .build()
                .unwrap(),
        );
        let root = ParquetType::group_type_builder("schema")
            .with_fields(vec![field])
            .build()
            .unwrap();
        let descr = SchemaDescriptor::new(std::sync::Arc::new(root));
        let cc = ColumnChunkMetaData::builder(descr.column(0)).set_statistics(Statistics::int64(
            Some(lo),
            Some(hi),
            None,
            Some(nulls),
            false,
        ));
        RowGroupMetaData::builder(std::sync::Arc::new(descr))
            .set_num_rows(rows)
            .set_column_metadata(vec![cc.build().unwrap()])
            .build()
            .unwrap()
    }

    fn cmp_pf(op: &str, lit: i64, col_first: bool) -> PushedFilter {
        let args = if col_first {
            vec![col_ref(0), i64_literal(lit)]
        } else {
            vec![i64_literal(lit), col_ref(0)]
        };
        let bytes = extended(&[(1, op)], &["a"], scalar_fn(1, args));
        PushedFilter::decode(&bytes).unwrap().unwrap()
    }

    #[test]
    fn eng47483_minmax_equality() {
        let rg = rg_int_range(100, "a", 10, 20, 0);
        assert!(
            !cmp_pf("equal:any_any", 5, true).group_can_match_pub(&rg),
            "5 < min"
        );
        assert!(
            !cmp_pf("equal:any_any", 25, true).group_can_match_pub(&rg),
            "25 > max"
        );
        assert!(cmp_pf("equal:any_any", 15, true).group_can_match_pub(&rg));
        // Boundaries are inclusive; excluding them would drop live rows.
        assert!(cmp_pf("equal:any_any", 10, true).group_can_match_pub(&rg));
        assert!(cmp_pf("equal:any_any", 20, true).group_can_match_pub(&rg));
    }

    #[test]
    fn eng47483_minmax_ranges_at_the_boundary() {
        let rg = rg_int_range(100, "a", 10, 20, 0);
        // a > 20 is impossible; a > 19 is not.
        assert!(!cmp_pf("gt:any_any", 20, true).group_can_match_pub(&rg));
        assert!(cmp_pf("gt:any_any", 19, true).group_can_match_pub(&rg));
        // a >= 21 impossible; a >= 20 possible.
        assert!(!cmp_pf("gte:any_any", 21, true).group_can_match_pub(&rg));
        assert!(cmp_pf("gte:any_any", 20, true).group_can_match_pub(&rg));
        // a < 10 impossible; a < 11 possible.
        assert!(!cmp_pf("lt:any_any", 10, true).group_can_match_pub(&rg));
        assert!(cmp_pf("lt:any_any", 11, true).group_can_match_pub(&rg));
        // a <= 9 impossible; a <= 10 possible.
        assert!(!cmp_pf("lte:any_any", 9, true).group_can_match_pub(&rg));
        assert!(cmp_pf("lte:any_any", 10, true).group_can_match_pub(&rg));
    }

    #[test]
    fn eng47483_minmax_reversed_operands() {
        // `20 < a` is `a > 20`. Getting the mirror wrong inverts the test and
        // drops exactly the groups that match.
        let rg = rg_int_range(100, "a", 10, 20, 0);
        assert!(
            !cmp_pf("lt:any_any", 20, false).group_can_match_pub(&rg),
            "20 < a impossible"
        );
        assert!(
            cmp_pf("lt:any_any", 5, false).group_can_match_pub(&rg),
            "5 < a possible"
        );
        assert!(
            !cmp_pf("gt:any_any", 10, false).group_can_match_pub(&rg),
            "10 > a impossible"
        );
        assert!(
            cmp_pf("gt:any_any", 25, false).group_can_match_pub(&rg),
            "25 > a possible"
        );
    }

    #[test]
    fn eng47483_not_equal_only_prunes_a_constant_group() {
        assert!(
            !cmp_pf("not_equal:any_any", 7, true)
                .group_can_match_pub(&rg_int_range(100, "a", 7, 7, 0))
        );
        assert!(
            cmp_pf("not_equal:any_any", 7, true)
                .group_can_match_pub(&rg_int_range(100, "a", 7, 8, 0))
        );
    }

    #[test]
    fn eng47483_all_null_group_matches_no_comparison() {
        // Bounds say 10..20, but every row is NULL and NULL satisfies nothing.
        let rg = rg_int_range(100, "a", 10, 20, 100);
        assert!(!cmp_pf("equal:any_any", 15, true).group_can_match_pub(&rg));
    }

    #[test]
    fn eng47483_float_literal_against_int_column_keeps_the_group() {
        // Coercing 15.5 to an integer here would round, and rounding the wrong
        // way drops live rows. Not coerced: keep.
        let rg = rg_int_range(100, "a", 10, 20, 0);
        let bytes = extended(
            &[(1, "gt:any_any")],
            &["a"],
            scalar_fn(1, vec![col_ref(0), f64_literal(25.5)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert!(pf.group_can_match_pub(&rg));
    }

    // ── ENG-47483 review findings R1 / R2 ────────────────────────────────

    /// Wrap row groups into a `ParquetMetaData` so `select_row_groups` — the
    /// whole-file entry point — can be exercised, not just the per-group
    /// predicate. All groups must share one schema descriptor, which
    /// `rg_int_range` already guarantees by building an identical one each time.
    fn meta_of(groups: Vec<RowGroupMetaData>) -> ParquetMetaData {
        use parquet::file::metadata::{FileMetaData, ParquetMetaDataBuilder};
        let descr = groups[0].schema_descr_ptr();
        let num_rows = groups.iter().map(|g| g.num_rows()).sum();
        ParquetMetaDataBuilder::new(FileMetaData::new(1, num_rows, None, None, descr, None))
            .set_row_groups(groups)
            .build()
    }

    #[test]
    fn select_row_groups_reports_all_none_and_some_distinctly() {
        // Three outcomes, three different return shapes, and the caller in
        // `Storage::create_parquet_file_stream` treats each differently — so
        // assert all three rather than only the partial case.
        //
        // The empty-selection case is the one that matters: `Some(vec![])` is a
        // legitimate "no group can match" verdict that must reach
        // `with_row_groups` and yield zero rows, and it is also exactly the
        // shape the R1 empty-OR bug produced by accident. A test that only
        // covers partial pruning cannot tell a correct empty selection from a
        // regression that manufactures one.
        let groups = || {
            vec![
                rg_int_range(100, "a", 0, 10, 0),
                rg_int_range(100, "a", 20, 30, 0),
                rg_int_range(100, "a", 40, 50, 0),
            ]
        };

        // `a > 100` — above every max, so nothing can match anywhere.
        assert_eq!(
            cmp_pf("gt:any_any", 100, true).select_row_groups(&meta_of(groups())),
            Some(vec![]),
            "a predicate no group can satisfy must return an EMPTY selection, \
             not None; None means 'read everything' and would scan the file"
        );

        // `a > 15` — group 0 (max 10) cannot match; groups 1 and 2 can.
        assert_eq!(
            cmp_pf("gt:any_any", 15, true).select_row_groups(&meta_of(groups())),
            Some(vec![1, 2]),
            "indices must be the KEPT groups, in file order"
        );

        // `a > -1` — every group can match, so decline to call with_row_groups
        // at all rather than passing the full list.
        assert_eq!(
            cmp_pf("gt:any_any", -1, true).select_row_groups(&meta_of(groups())),
            None,
            "nothing prunable must be None, so the caller skips with_row_groups"
        );
    }

    #[test]
    fn empty_or_keeps_the_group_instead_of_pruning_everything() {
        // R1. `any` on an empty iterator is FALSE, so an unguarded empty OR
        // prunes every row group: `select_row_groups` returns an empty
        // selection, `with_row_groups` fetches nothing, and the query returns
        // no rows with no error. `boolean_or` refuses an empty OR at eval time
        // for exactly this reason; the pruner has to agree.
        let bytes = extended(&[(1, "or:bool")], &["a"], scalar_fn(1, vec![]));
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert!(
            pf.group_can_match(&pf.expression, &rg_meta(100, "a", Some(0))),
            "a malformed empty OR must KEEP the group; pruning it loses every \
             row with no diagnostic"
        );
        // And at the level the bug actually manifests: an unguarded empty OR
        // returns `Some(vec![])` here, which reads as a legitimate "no group
        // can match" and silently returns no rows.
        assert_eq!(
            pf.select_row_groups(&meta_of(vec![
                rg_int_range(100, "a", 0, 10, 0),
                rg_int_range(100, "a", 20, 30, 0),
            ])),
            None,
            "an empty OR must prune nothing at all, so the caller never calls \
             with_row_groups"
        );
    }

    #[test]
    fn ambiguous_column_name_keeps_the_group() {
        // R2. Statistics are looked up by LEAF name, case-insensitively. Two
        // columns whose leaf names differ only in case both match, and taking
        // whichever comes first would prune on the wrong column's statistics.
        // Not being able to tell which column is meant means keep.
        use parquet::basic::Type as PT;
        use parquet::file::metadata::ColumnChunkMetaData;
        use parquet::file::statistics::Statistics;
        let fields = ["a", "A"]
            .iter()
            .map(|n| {
                std::sync::Arc::new(
                    ParquetType::primitive_type_builder(n, PT::INT64)
                        .build()
                        .unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let root = ParquetType::group_type_builder("schema")
            .with_fields(fields)
            .build()
            .unwrap();
        let descr = std::sync::Arc::new(SchemaDescriptor::new(std::sync::Arc::new(root)));
        // Both columns report null_count == 0, which alone would prune IS NULL.
        let cols: Vec<_> = (0..2)
            .map(|i| {
                ColumnChunkMetaData::builder(descr.column(i))
                    .set_statistics(Statistics::int64(None, None, None, Some(0), false))
                    .build()
                    .unwrap()
            })
            .collect();
        let rg = RowGroupMetaData::builder(descr)
            .set_num_rows(100)
            .set_column_metadata(cols)
            .build()
            .unwrap();

        let bytes = extended(
            &[(1, "is_null:any")],
            &["a"],
            scalar_fn(1, vec![col_ref(0)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        assert!(
            pf.group_can_match(&pf.expression, &rg),
            "'a' and 'A' both match case-insensitively, so the column is \
             ambiguous and the group must be kept"
        );
    }

    #[test]
    fn pushdown_eng47480_keeps_pure_isnull_predicate() {
        // Predicate: isnull(col 0). Keeps only the null fraction — typically a
        // few percent — so the second decode pass is repaid many times over.
        let bytes = extended(
            &[(1, "is_null:any")],
            &["a", "b"],
            scalar_fn(1, vec![col_ref(0)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let schema = make_parquet_schema(&["a", "b"]);
        assert!(
            pf.build_row_filter(&schema).is_some(),
            "pure IS NULL is highly selective and must install a RowFilter"
        );
    }

    #[test]
    fn pushdown_eng47480_keeps_q76_shape() {
        // The exact shape q76 pushes at all three fact tables:
        //   isnull(ss_customer_sk) AND isnotnull(ss_item_sk)
        // The IsNotNull conjunct is inert, but the IsNull conjunct rejects
        // 97.6-100.0% of rows, so the AND as a whole is worth filtering on.
        let bytes = extended(
            &[
                (1, "is_null:any"),
                (2, "is_not_null:any"),
                (3, "and:bool_bool"),
            ],
            &["a", "b"],
            scalar_fn(
                3,
                vec![
                    scalar_fn(1, vec![col_ref(0)]),
                    scalar_fn(2, vec![col_ref(1)]),
                ],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let schema = make_parquet_schema(&["a", "b"]);
        assert!(
            pf.build_row_filter(&schema).is_some(),
            "IS NULL AND IS NOT NULL must install: the IS NULL conjunct carries it"
        );
    }

    #[test]
    fn pushdown_eng47480_negated_isnull_is_treated_as_isnotnull() {
        // NOT(isnull(a)) is isnotnull(a) semantically. Classifying it off the
        // bare node name would install a filter that rejects ~2-3% of rows and
        // pays a full second pass for it.
        let bytes = extended(
            &[(1, "is_null:any"), (2, "not")],
            &["a", "b"],
            scalar_fn(2, vec![scalar_fn(1, vec![col_ref(0)])]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let schema = make_parquet_schema(&["a", "b"]);
        assert!(
            pf.build_row_filter(&schema).is_none(),
            "NOT(IS NULL) is IS NOT NULL and must be skipped"
        );
    }

    #[test]
    fn pushdown_eng47480_negated_isnotnull_is_treated_as_isnull() {
        // The mirror: NOT(isnotnull(a)) is isnull(a), and must install.
        let bytes = extended(
            &[(1, "is_not_null:any"), (2, "not")],
            &["a", "b"],
            scalar_fn(2, vec![scalar_fn(1, vec![col_ref(0)])]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let schema = make_parquet_schema(&["a", "b"]);
        assert!(
            pf.build_row_filter(&schema).is_some(),
            "NOT(IS NOT NULL) is IS NULL and must install a RowFilter"
        );
    }

    #[test]
    fn pushdown_v4_1_keeps_mixed_predicate_with_comparison() {
        // Predicate: isnotnull(a) AND (b > 5). The comparison branch CAN
        // prune via stats, so the AND as a whole is prunable. Pushdown
        // should still be installed.
        let bytes = extended(
            &[
                (1, "is_not_null:any"),
                (2, "gt:any_any"),
                (3, "and:bool_bool"),
            ],
            &["a", "b"],
            scalar_fn(
                3,
                vec![
                    scalar_fn(1, vec![col_ref(0)]),
                    scalar_fn(2, vec![col_ref(1), i64_literal(5)]),
                ],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let schema = make_parquet_schema(&["a", "b"]);
        let rf = pf.build_row_filter(&schema);
        assert!(
            rf.is_some(),
            "mixed isnotnull AND comparison should still install (comparison can prune)"
        );
    }

    #[test]
    fn pushdown_arrow_predicate_returns_all_true_on_eval_error() {
        // Predicate references column 'z', but batch only has 'a','b'.
        // PushedFilter::evaluate will Err. The ArrowPredicate adapter must
        // catch this and return an all-true mask (so parquet doesn't fail
        // the whole read; the post-merge filter handles correctness).
        let batch = make_batch_two_int_cols();
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["z"],
            scalar_fn(1, vec![col_ref(0), i64_literal(5)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        // Parquet schema is independent of the test batch — projection mask
        // points at a fictitious column for the wrapper's bookkeeping.
        let schema = make_parquet_schema(&["z"]);
        let projection = ProjectionMask::roots(&schema, vec![0_usize]);
        let mut pred = PushedFilterArrowPredicate {
            filter: pf,
            projection,
        };
        let mask = pred
            .evaluate(batch.clone())
            .expect("adapter must not propagate the error");
        assert_eq!(mask.len(), batch.num_rows());
        assert!(
            (0..mask.len()).all(|i| mask.value(i)),
            "adapter must return all-true mask on eval failure"
        );
    }

    // ════════════════════════════════════════════════════════════════════
    // ENG-40156 — degenerate combinators must Err (not under-include)
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn test_empty_boolean_and_errors() {
        // An empty AND used to return an all-true mask — which a wrapping NOT()
        // flips to all-false, silently under-including. It must Err so the
        // SITE-1/SITE-2 fallbacks over-include + warn instead.
        assert!(boolean_and(vec![], 4).is_err());
    }

    #[test]
    fn test_empty_boolean_or_errors() {
        // An empty OR used to return an all-false mask — which prunes ALL rows
        // at the parquet RowFilter (unrecoverable under-include). It must Err.
        assert!(boolean_or(vec![], 4).is_err());
    }

    #[test]
    fn test_malformed_filter_bytes_decodes_to_none() {
        // Clearly-invalid protobuf bytes. decode() returns Err at the decode
        // layer (the lib.rs caller downgrades it to a dropped filter + warn).
        let res = PushedFilter::decode(&[0xff, 0xff, 0xff, 0xff]);
        assert!(
            res.is_err(),
            "malformed protobuf bytes should Err at the decode layer; got {res:?}"
        );
    }

    // ════════════════════════════════════════════════════════════════════
    // ENG-40156 — function validation is scoped to REFERENCED functions.
    // Gluten serialises the whole plan's function table into the blob, so an
    // unrelated declaration must not drop a perfectly supported predicate.
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn unreferenced_unknown_declaration_keeps_pushdown() {
        let batch = make_batch_two_int_cols();
        // Plan-wide table declares `sum` (aggregate, not evaluable here) at
        // anchor 1, but the pushed expression only calls `gt` at anchor 0.
        let bytes = extended(
            &[(0, "gt:any_any"), (1, "sum:opt_fp64")],
            &["a", "b"],
            scalar_fn(0, vec![col_ref(0), i64_literal(2)]),
        );
        let pf = PushedFilter::decode(&bytes)
            .unwrap()
            .expect("unreferenced unknown declaration must not drop the filter");
        assert_eq!(pf.function_map.get(&0), Some(&KnownFunction::Gt));
        assert_eq!(
            pf.function_map.get(&1),
            None,
            "unknown declaration must be skipped, not mapped"
        );

        // a > 2 → rows 2(a=3) and 4(a=5); row 3 is NULL → NULL → dropped.
        let mask = pf.evaluate(&batch).unwrap();
        assert!(!mask.value(0), "a=1 is not > 2");
        assert!(!mask.value(1), "a=2 is not > 2");
        assert!(mask.value(2), "a=3 is > 2");
        assert!(mask.is_null(3), "NULL column value must yield NULL mask");
        assert!(mask.value(4), "a=5 is > 2");

        let filtered = filter_batch(&batch, &pf).unwrap();
        let a = filtered
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(a.values(), &[3, 5]);
    }

    #[test]
    fn referenced_unknown_function_drops_pushdown() {
        // `gt` is declared and evaluable, but the pushed expression itself calls
        // `substring` — that one we can't evaluate, so drop the whole filter.
        let bytes = extended(
            &[(0, "gt:any_any"), (1, "substring:str_i32_i32")],
            &["a", "b"],
            scalar_fn(
                0,
                vec![
                    scalar_fn(1, vec![col_ref(0), i64_literal(1)]),
                    i64_literal(2),
                ],
            ),
        );
        assert!(
            PushedFilter::decode(&bytes).unwrap().is_none(),
            "a referenced unknown function must drop the filter"
        );
    }

    #[test]
    fn referenced_undeclared_anchor_drops_pushdown() {
        // Expression calls anchor 9, which has no declaration at all.
        let bytes = extended(
            &[(0, "gt:any_any")],
            &["a", "b"],
            scalar_fn(9, vec![col_ref(0), i64_literal(2)]),
        );
        assert!(
            PushedFilter::decode(&bytes).unwrap().is_none(),
            "a referenced but undeclared anchor must drop the filter"
        );
    }

    // ════════════════════════════════════════════════════════════════════
    // ENG-47570 — signed zero.
    //
    // Arrow compares floats by IEEE-754 totalOrder: `is_eq` is literally
    // `to_bits() == to_bits()`, so `-0.0` and `0.0` are distinct and `-0.0`
    // sorts below `0.0`. Spark compares after `NormalizeNaNAndZero` and holds
    // `-0.0 = 0.0` true. Uncorrected, the kernels DROP a row holding `-0.0`
    // from `c = 0.0`, and a dropped row is unrecoverable — Velox's post-scan
    // filter re-evaluates on the rows we return, so it can only remove more.
    //
    // Every test below fails on a8d3d26 (the un-fixed vectorization) except
    // the two that pin behaviour which must NOT change.
    // ════════════════════════════════════════════════════════════════════

    /// One f64 column named `c`, with the given values.
    fn f64_batch(values: Vec<Option<f64>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Float64, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(values))]).unwrap()
    }

    /// Decode `c <op> literal` and return the row mask over `batch`.
    fn mask_for(batch: &RecordBatch, op: &str, literal: Expression) -> BooleanArray {
        let bytes = extended(&[(1, op)], &["c"], scalar_fn(1, vec![col_ref(0), literal]));
        PushedFilter::decode(&bytes)
            .expect("must decode")
            .expect("must not drop the filter")
            .evaluate(batch)
            .expect("must evaluate")
    }

    #[test]
    fn equal_zero_matches_negative_zero_f64() {
        let batch = f64_batch(vec![Some(-0.0), Some(0.0), Some(1.0)]);
        let mask = mask_for(&batch, "equal:any_any", f64_literal(0.0));
        assert!(
            mask.value(0),
            "-0.0 = 0.0 is true in Spark; dropping this row loses it silently"
        );
        assert!(mask.value(1), "0.0 = 0.0");
        assert!(!mask.value(2), "1.0 != 0.0");
    }

    #[test]
    fn equal_negative_zero_literal_matches_positive_zero_f64() {
        // The mirror case: the literal carries the sign, the column does not.
        let batch = f64_batch(vec![Some(0.0), Some(-0.0)]);
        let mask = mask_for(&batch, "equal:any_any", f64_literal(-0.0));
        assert!(mask.value(0), "0.0 = -0.0 is true in Spark");
        assert!(mask.value(1), "-0.0 = -0.0");
    }

    #[test]
    fn gte_zero_keeps_negative_zero_f64() {
        // totalOrder puts -0.0 strictly below 0.0, so an uncorrected `>=`
        // drops it. This is the shape most likely to appear in real SQL:
        // `WHERE amount >= 0`.
        let batch = f64_batch(vec![Some(-0.0), Some(-1.0), Some(0.0), Some(2.0)]);
        let mask = mask_for(&batch, "gte:any_any", f64_literal(0.0));
        assert!(mask.value(0), "-0.0 >= 0.0 is true in Spark");
        assert!(!mask.value(1), "-1.0 is not >= 0.0");
        assert!(mask.value(2), "0.0 >= 0.0");
        assert!(mask.value(3), "2.0 >= 0.0");
    }

    #[test]
    fn lte_negative_zero_keeps_positive_zero_f64() {
        let batch = f64_batch(vec![Some(0.0), Some(1.0)]);
        let mask = mask_for(&batch, "lte:any_any", f64_literal(-0.0));
        assert!(mask.value(0), "0.0 <= -0.0 is true in Spark");
        assert!(!mask.value(1), "1.0 is not <= -0.0");
    }

    #[test]
    fn gt_zero_still_excludes_negative_zero_f64() {
        // Normalizing must not turn `>` into `>=`: once both sides are +0.0,
        // a strict comparison still rejects.
        let batch = f64_batch(vec![Some(-0.0), Some(0.0), Some(0.5)]);
        let mask = mask_for(&batch, "gt:any_any", f64_literal(0.0));
        assert!(!mask.value(0), "-0.0 > 0.0 is false");
        assert!(!mask.value(1), "0.0 > 0.0 is false");
        assert!(mask.value(2), "0.5 > 0.0");
    }

    #[test]
    fn equal_zero_matches_negative_zero_f32() {
        let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Float32, true)]));
        let a = Float32Array::from(vec![Some(-0.0_f32), Some(0.0), Some(1.0)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(a)]).unwrap();
        let mask = mask_for(&batch, "equal:any_any", f32_literal(0.0_f32));
        assert!(mask.value(0), "-0.0f32 = 0.0f32 is true in Spark");
        assert!(mask.value(1), "0.0f32 = 0.0f32");
        assert!(!mask.value(2), "1.0f32 != 0.0f32");
    }

    #[test]
    fn in_list_with_zero_option_matches_negative_zero() {
        // `eval_singular_or_list` folds equality comparisons through the same
        // arm, so IN inherits the correction rather than needing its own.
        let batch = f64_batch(vec![Some(-0.0), Some(7.0), Some(3.0)]);
        let bytes = extended(
            &[],
            &["c"],
            in_list(col_ref(0), vec![f64_literal(0.0), f64_literal(7.0)]),
        );
        let mask = PushedFilter::decode(&bytes)
            .unwrap()
            .unwrap()
            .evaluate(&batch)
            .unwrap();
        assert!(mask.value(0), "-0.0 IN (0.0, 7.0) is true in Spark");
        assert!(mask.value(1), "7.0 IN (0.0, 7.0)");
        assert!(!mask.value(2), "3.0 NOT IN (0.0, 7.0)");
    }

    #[test]
    fn reversed_operands_normalize_too_f64() {
        // `0.0 <= c` rather than `c >= 0.0`. `comparison()` routes a
        // scalar-then-column argument pair through the same
        // `compare_column_scalar` with `reversed = true`, and normalization
        // has to happen before the operands are swapped — otherwise half the
        // surface stays broken while the tests above all pass.
        let batch = f64_batch(vec![Some(-0.0), Some(-1.0), Some(2.0)]);
        let bytes = extended(
            &[(1, "lte:any_any")],
            &["c"],
            scalar_fn(1, vec![f64_literal(0.0), col_ref(0)]),
        );
        let mask = PushedFilter::decode(&bytes)
            .unwrap()
            .unwrap()
            .evaluate(&batch)
            .unwrap();
        assert!(mask.value(0), "0.0 <= -0.0 is true in Spark");
        assert!(!mask.value(1), "0.0 is not <= -1.0");
        assert!(mask.value(2), "0.0 <= 2.0");
    }

    #[test]
    fn nan_semantics_are_arrow_totalorder_and_match_spark() {
        // PASSES before the fix — a pin, not a repair. Spark's EqualTo on
        // doubles generates `(isNaN(a) && isNaN(b)) || a == b`, and orders NaN
        // above every non-NaN; arrow's totalOrder already does both. The old
        // per-row `PartialOrd` loop did neither, so the vectorization FIXED
        // two under-includes here. Nothing in the signed-zero correction may
        // regress that: `v == 0.0` is false for NaN, so NaN is never touched.
        let batch = f64_batch(vec![Some(f64::NAN), Some(1.0)]);

        let mask = mask_for(&batch, "equal:any_any", f64_literal(f64::NAN));
        assert!(mask.value(0), "NaN = NaN is true in Spark");
        assert!(!mask.value(1), "1.0 != NaN");

        let mask = mask_for(&batch, "gt:any_any", f64_literal(0.0));
        assert!(
            mask.value(0),
            "NaN > 0.0 is true in Spark (NaN sorts highest)"
        );
        assert!(mask.value(1), "1.0 > 0.0");
    }

    #[test]
    fn non_zero_literal_is_left_alone() {
        // PASSES before the fix. The correction is gated on the literal being
        // a zero, because -0.0 and 0.0 order identically against every other
        // literal — this pins that the gate costs nothing elsewhere.
        let batch = f64_batch(vec![Some(-0.0), Some(-1.0), Some(1.0), None]);
        let mask = mask_for(&batch, "gt:any_any", f64_literal(-0.5));
        assert!(mask.value(0), "-0.0 > -0.5");
        assert!(!mask.value(1), "-1.0 is not > -0.5");
        assert!(mask.value(2), "1.0 > -0.5");
        assert!(mask.is_null(3), "NULL stays NULL, not false");
    }
}
