//! Convert a query plan to a polars target plan.
//! This is the implementation of Target Plans for Workers.
use std::ops::{Add, Div, Mul, Sub};

use polars::error::PolarsError;
use polars::lazy::prelude::*;
use polars::prelude::*;

use substrait::proto::{
    expression, expression::field_reference, expression::literal::LiteralType, Expression, Rel,
};

use qplan::plan::*;

/// Errors that can occur when creating this specific target plan.
#[derive(Debug, thiserror::Error)]
pub enum PolarsTargetErr {
    #[error("cannot compile plan: {0}")]
    PolarsTarget(String),
    #[error("traversal context error: {0}")]
    Plan(#[from] PlanErr),
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Polars error: {0}")]
    Polars(#[from] PolarsError),
    #[error("Cannot convert integer: {0}")]
    IntError(#[from] std::num::TryFromIntError),
}

/// Helper to generate PolarsTargetErr
fn polars_err<T>(msg: String) -> Result<T, PolarsTargetErr> {
    Err(PolarsTargetErr::PolarsTarget(msg))
}

/// The result type for this specific target plan.
pub type PolarsTargetResult<T> = Result<T, PolarsTargetErr>;

/// Interface to the datastore; we use a parquet reader in production
/// and a mock that just produces predefined data for testing.
pub trait DataStore {
    fn get_data_source(
        &self,
        dataset: &str,
        table: &str,
        chunks: &[String],
    ) -> PolarsTargetResult<LazyFrame>;
}

/// Trait implementation for Target Plan.
/// The implementation defines what we essentially need from the original
/// substrait plan to generate this kind of plan, we are, for example,
/// not interested in joins in the polars target.
#[derive(Clone, Debug)]
pub enum PolarsTarget {
    Empty,
    Relation(RelationType, Vec<Expression>, Box<PolarsTarget>),
    Source(Source),
}

impl PolarsTarget {
    /// Compiles the Target Plan to Polars expressions.
    pub fn compile<S: DataStore>(
        &self,
        tctx: &TraversalContext,
        bucket: &str,
        chunk_ids: &[String],
        ds: &S,
    ) -> PolarsTargetResult<Option<LazyFrame>> {
        let src = self.get_source();
        if src.is_none() {
            return Ok(None);
        }
        let src = src.unwrap();
        match self {
            PolarsTarget::Empty => Ok(None),
            PolarsTarget::Relation(rt, x, kid) => {
                if let Some(input) = kid.compile(tctx, bucket, chunk_ids, ds)? {
                    compile_relation(rt, src, &x, tctx, input)
                } else {
                    Ok(None)
                }
            }
            PolarsTarget::Source(src) => {
                if !src.sqd {
                    Ok(None)
                } else {
                    let lf = ds.get_data_source(&bucket, &src.table_name, chunk_ids)?;
                    Ok(Some(lf))
                }
            }
        }
    }
}

fn compile_relation(
    rt: &RelationType,
    src: &Source,
    x: &[Expression],
    tctx: &TraversalContext,
    lf: LazyFrame,
) -> PolarsTargetResult<Option<LazyFrame>> {
    let lf = match rt {
        RelationType::Projection => compile_projection(src, x, tctx, lf)?,
        RelationType::Filter if x.len() > 0 => {
            lf.filter(PolarsExprTransformer::from_filter().transform_expr(&x[0], src, tctx)?)
        }
        RelationType::Filter => lf,
        RelationType::Fetch(_, c) => lf.limit(*c as u32), //TODO: do the conversion in Fetch!
        RelationType::Other(s) if s == &"reduced join" => lf,
        RelationType::Other(_) => lf,
    };
    Ok(Some(lf))
}

fn compile_projection(
    src: &Source,
    x: &[Expression],
    tctx: &TraversalContext,
    lf: LazyFrame,
) -> PolarsTargetResult<LazyFrame> {
    Ok(if !src.projection.is_empty() {
        lf.select(project_from_source(src, tctx)?)
    } else {
        lf.select(convert_expressions(x, src, tctx)?)
    })
}

fn project_from_source(src: &Source, _tctx: &TraversalContext) -> PolarsTargetResult<Vec<Expr>> {
    Ok(map_through_projection(src))
}

fn map_through_projection(src: &Source) -> Vec<Expr> {
    let mut v = Vec::new();
    for idx in &src.projection {
        if *idx < src.fields.len() {
            // This is a very cheap workaround for a non-supported serialisation in polars-json
            // It does not support BLOB.
            if src.fields[*idx] == "accounts_bloom" {
                v.push(lit("ignore").alias("accounts_bloom"));
            } else {
                v.push(col(src.fields[*idx].clone()));
            }
        }
    }
    v
}

impl TargetPlan for PolarsTarget {
    /// Leaf.
    fn empty() -> Self {
        PolarsTarget::Empty
    }

    /// Store a generic relation in the Target Plan.
    fn from_relation(
        relt: RelationType,
        exps: &[Expression],
        _from: &Rel,
        rel: Self,
    ) -> PlanResult<Self> {
        Ok(PolarsTarget::Relation(relt, exps.to_vec(), Box::new(rel)))
    }

    /// Queries containing joins generate an error, because we don't handle joins locally!
    fn from_join(_exps: &[Expression], _from: &Rel, _left: Self, _right: Self) -> PlanResult<Self> {
        plan_err("joins are not supported on worker side".to_string())
    }

    /// Store the source in the Target Plan.
    fn from_source(source: Source) -> Self {
        PolarsTarget::Source(source)
    }

    /// Get the source (since we have no joins, there is only one source).
    fn get_source(&self) -> Option<&Source> {
        match self {
            PolarsTarget::Empty => None,
            PolarsTarget::Relation(_, _, kid) => kid.get_source(),
            PolarsTarget::Source(src) => Some(src),
        }
    }

    /// Since we don't support joins on worker side,
    /// we don't need to bother about getting more than one source.
    fn get_sources(&self) -> Vec<Source> {
        Vec::with_capacity(0)
    }
}

// Get the expressions for the Select context
// Careful! We need to check if the expression actually relates to the source!
// We usually use the source.projection, only if that is None we use the projection rel.
// The only case when that *should* happen is when there is only one source in the query anyway.
fn convert_expressions(
    xs: &[Expression],
    source: &Source,
    tctx: &TraversalContext,
) -> PolarsTargetResult<Vec<Expr>> {
    let mut v = Vec::new();
    for x in xs {
        v.push(PolarsExprTransformer::from_projection().transform_expr(x, source, tctx)?);
    }
    Ok(v)
}

enum LitType {
    LiteralInt,
    LiteralStr,
}

struct PolarsExprTransformer {
    convert_tp_to_str: bool,
    convert_str_to_tp: bool,
}

impl PolarsExprTransformer {
    fn from_filter() -> PolarsExprTransformer {
        PolarsExprTransformer {
            convert_tp_to_str: false,
            convert_str_to_tp: true,
        }
    }
    fn from_projection() -> PolarsExprTransformer {
        PolarsExprTransformer {
            convert_tp_to_str: true,
            convert_str_to_tp: false,
        }
    }
    fn literal_list_to_series(
        &self,
        es: &[Expression],
        s: &Source,
        tctx: &TraversalContext,
    ) -> PolarsTargetResult<Series> {
        let t = match self.transform_expr(&es[0], s, tctx)? {
            Expr::Literal(LiteralValue::Int(_)) => Ok(LitType::LiteralInt),
            Expr::Literal(LiteralValue::String(_)) => Ok(LitType::LiteralStr),
            _ => polars_err("unexpected literal".to_string()),
        }?;

        match t {
            LitType::LiteralInt => {
                let mut os = Vec::new();
                for e in es.iter() {
                    os.push(Self::expr_to_int(&self.transform_expr(e, s, tctx)?)?);
                }
                Ok(polars::series::Series::new("".into(), os))
            }
            LitType::LiteralStr => {
                let mut os = Vec::new();
                for e in es.iter() {
                    os.push(Self::expr_to_str(&self.transform_expr(e, s, tctx)?)?);
                }
                Ok(polars::series::Series::new("".into(), os))
            }
        }
    }

    fn expr_to_int(e: &Expr) -> PolarsTargetResult<i128> {
        match e {
            Expr::Literal(LiteralValue::Int(i)) => Ok(*i),
            _ => polars_err("unexpected literal".to_string()),
        }
    }

    // TODO: string or timestamp
    fn expr_to_str(e: &Expr) -> PolarsTargetResult<String> {
        match e {
            Expr::Literal(LiteralValue::String(s)) => Ok(s.to_string()),
            _ => polars_err("unexpected literal".to_string()),
        }
    }

    // TODO: we won't need that in the future, the conversion is done by duckdb anyway.
    fn get_col_from_source(&self, source: &Source, r: i32) -> PolarsTargetResult<Expr> {
        let nm = Self::get_field_name_from_source(source, r)?;
        if self.convert_tp_to_str && TIMESTAMP_FIELD_NAMES.contains(&nm.as_str()) {
            Ok(col(nm).dt().strftime("%Y-%m-%d %H:%M:%S"))
        } else {
            Ok(col(nm))
        }
    }

    fn string_or_timestamp(&self, s: &str) -> Result<Expr, PolarsTargetErr> {
        if self.convert_str_to_tp {
            match chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                Ok(t) => Ok(lit(t.and_utc().timestamp())),
                _ => Ok(lit(s.to_string())),
            }
        } else {
            Ok(lit(s.to_string()))
        }
    }
}

impl ExprTransformer<Expr, PolarsTargetErr> for PolarsExprTransformer {
    fn err_producer<T>(msg: String) -> Result<T, PolarsTargetErr> {
        polars_err(msg)
    }

    fn transform_literal(&self, l: &expression::Literal) -> Result<Expr, PolarsTargetErr> {
        match l.literal_type {
            Some(LiteralType::Boolean(b)) => Ok(lit(b)),
            Some(LiteralType::I32(i)) => Ok(lit(i)),
            Some(LiteralType::I64(i)) => Ok(lit(i)),
            Some(LiteralType::String(ref s)) => self.string_or_timestamp(s),
            _ => Self::err_producer("unsupported literal type".to_string()),
        }
    }

    fn transform_selection(
        &self,
        _tctx: &TraversalContext,
        source: &Source,
        f: &expression::FieldReference,
    ) -> Result<Expr, PolarsTargetErr> {
        match f.reference_type {
            Some(field_reference::ReferenceType::DirectReference(ref s)) => map_on_dirref(
                source,
                s,
                |s, r| self.get_col_from_source(s, r),
                |m| Self::err_producer(m),
            ),
            _ => Self::err_producer("unsupported reference type".to_string()),
        }
    }

    fn transform_fun(
        &self,
        tctx: &TraversalContext,
        source: &Source,
        f: &expression::ScalarFunction,
    ) -> Result<Expr, PolarsTargetErr> {
        let fun = tctx.get_fun_from_ext(f)?;
        let args = self.get_fun_args(tctx, source, &f.arguments)?;

        connect_expr(fun, args)
    }

    fn transform_list(
        &self,
        tctx: &TraversalContext,
        source: &Source,
        l: &expression::SingularOrList,
    ) -> Result<Expr, PolarsTargetErr> {
        let field = match l.value {
            Some(ref x) => self.transform_expr(&*x, source, tctx),
            None => polars_err("no field in list expression".to_string()),
        }?;

        let values = self.literal_list_to_series(&l.options, source, tctx)?;

        Ok(field.is_in(lit(values).implode()))
    }
}

// TODO: add all relevant functions
fn connect_expr(fun: Ext, args: Vec<Expr>) -> PolarsTargetResult<Expr> {
    tracing::debug!("{:?} {:?} -> ", fun, args);
    match fun {
        Ext::GT if args.len() >= 2 => Ok(args[0].clone().gt(args[1].clone())),
        Ext::LT if args.len() >= 2 => Ok(args[0].clone().lt(args[1].clone())),
        Ext::EQ if args.len() >= 2 => Ok(args[0].clone().eq(args[1].clone())),
        Ext::NE if args.len() >= 2 => Ok(args[0].clone().neq(args[1].clone())),
        Ext::GE if args.len() >= 2 => Ok(args[0].clone().gt_eq(args[1].clone())),
        Ext::LE if args.len() >= 2 => Ok(args[0].clone().lt_eq(args[1].clone())),
        Ext::And if args.len() >= 2 => Ok(args[0].clone().logical_and(args[1].clone())),
        Ext::Or if args.len() >= 2 => Ok(args[0].clone().logical_or(args[1].clone())),
        Ext::Add if args.len() >= 2 => Ok(args[0].clone().add(args[1].clone())),
        Ext::Sub if args.len() >= 2 => Ok(args[0].clone().sub(args[1].clone())),
        Ext::Mul if args.len() >= 2 => Ok(args[0].clone().mul(args[1].clone())),
        Ext::Div if args.len() >= 2 => Ok(args[0].clone().div(args[1].clone())),
        Ext::Not if args.len() >= 1 => Ok(args[0].clone().not()),
        unknown => polars_err(format!(
            "invalid or unknown function {:?} with {} args",
            unknown,
            args.len()
        )),
    }
}

// #[cfg(test)]
// mod test {
//     use super::*;
//     use polars::prelude::{DataFrame, df};
//     use substrait::proto::Plan;

//     struct TestDataStore;

//     impl DataStore for TestDataStore {
//         fn get_data_source(
//             &self,
//             _dataset: &str,
//             table_name: &str,
//             _chunks: &[String],
//         ) -> PolarsTargetResult<LazyFrame> {
//             if table_name == "block" {
//                 Ok(produce_block().lazy())
//             } else {
//                 polars_err(format!("unknown table: '{}'", table_name))
//             }
//         }
//     }

//     static JSON_PLAN_BLOCK_COLS: &str =
//         include_str!("../../../resources/block_plain_with_cols.json");
//     static JSON_PLAN_BLOCK_SORT: &str =
//         include_str!("../../../resources/block_simple_with_sort.json");
//     static JSON_PLAN_BLOCK_IN: &str = include_str!("../../../resources/block_plain_with_in.json");
//     static JSON_PLAN_BLOCK_COOL_JOIN: &str =
//         include_str!("../../../resources/block_simple_join.json");
//     static JSON_PLAN_BLOCK_SQD_JOIN: &str =
//         include_str!("../../../resources/block_remote_join2.json");

//     fn make_block_example_with_cols() -> Plan {
//         serde_json::from_str(JSON_PLAN_BLOCK_COLS).unwrap()
//     }

//     fn make_block_example_with_sort() -> Plan {
//         serde_json::from_str(JSON_PLAN_BLOCK_SORT).unwrap()
//     }

//     fn make_block_example_with_in() -> Plan {
//         serde_json::from_str(JSON_PLAN_BLOCK_IN).unwrap()
//     }

//     fn make_block_join_local_example() -> Plan {
//         serde_json::from_str(JSON_PLAN_BLOCK_COOL_JOIN).unwrap()
//     }

//     fn make_block_join_remote_example() -> Plan {
//         serde_json::from_str(JSON_PLAN_BLOCK_SQD_JOIN).unwrap()
//     }

//     #[allow(dead_code)] // for future reference
//     fn strftime_on_timestamp(lf: LazyFrame) -> DataFrame {
//         lf.with_columns([col("timestamp").dt().strftime("%Y-%m-%d %H:%M:%S")])
//             .collect()
//             .unwrap()
//     }

//     fn expected_simple() -> DataFrame {
//         df![
//             "number" => [217710084u32, 217710085, 217710086],
//             "timestamp" => [
//                 "2023-09-15 12:46:38",
//                 "2023-09-15 12:46:38",
//                 "2023-09-15 12:46:38",
//             ],
//         ]
//         .unwrap()
//     }

//     fn expected_sorted() -> DataFrame {
//         df![
//             "number" => [217710084u32, 217710085, 217710086],
//             "hash" => [
//                 "ACabnvdRdFYLutek6qjpjwkx2y6TBSnCMiFKhQF3SdSx",
//                 "5rcMrqfqra4eLe5Tcxf5beV8GFFQhWnX4kSmrjkeYQKY",
//                 "53aH29s69KerKrbuwRHBMCF2f5e37tD6v2SKMViYksQz",
//             ],
//             "timestamp" => [
//                 "2023-09-15 12:46:38",
//                 "2023-09-15 12:46:38",
//                 "2023-09-15 12:46:38",
//             ],
//         ]
//         .unwrap()
//     }

//     #[test]
//     fn test_plan_with_simple_block() {
//         let ts = TestDataStore;
//         let p = make_block_example_with_cols();
//         let mut tctx = TraversalContext::new(Default::default());
//         let target =
//             traverse_plan::<PolarsTarget>(&p, &mut tctx).expect("plan resulted in an error");
//         let df = target
//             .compile(&tctx, "", &vec![], &ts)
//             .expect("cannot compile target");
//         let have = df.unwrap().collect().expect("cannot run dataframe"); // strftime_on_timestamp(df.unwrap());

//         println!("{:?}", have);
//         println!("{:?}", expected_simple());

//         assert_eq!(have, expected_simple());
//     }

//     #[test]
//     // We don't support sorting on workers,
//     // but a plan with a sort should reduce to a working target
//     fn test_plan_with_sorted_block() {
//         let ts = TestDataStore;
//         let p = make_block_example_with_sort();
//         let mut tctx = TraversalContext::new(Default::default());
//         let target =
//             traverse_plan::<PolarsTarget>(&p, &mut tctx).expect("plan resulted in an error");
//         let df = target
//             .compile(&tctx, "", &vec![], &ts)
//             .expect("cannot compile target");
//         let have = df.unwrap().collect().expect("cannot run dataframe"); // strftime_on_timestamp(df.unwrap());

//         println!("{:?}", have);
//         println!("{:?}", expected_sorted());

//         assert_eq!(have, expected_sorted());
//     }

//     #[test]
//     fn test_plan_with_block_in() {
//         let ts = TestDataStore;
//         let p = make_block_example_with_in();
//         let mut tctx = TraversalContext::new(Default::default());
//         let target =
//             traverse_plan::<PolarsTarget>(&p, &mut tctx).expect("plan resulted in an error");
//         let df = target
//             .compile(&tctx, "", &vec![], &ts)
//             .expect("cannot compile target");
//         let have = df.unwrap().collect().expect("cannot run dataframe"); // strftime_on_timestamp(df.unwrap());

//         println!("{:?}", have);
//         println!("{:?}", expected_simple());

//         assert_eq!(have, expected_simple());
//     }

//     #[test]
//     fn test_plan_with_simple_join() {
//         let p = make_block_join_local_example();
//         let mut tctx = TraversalContext::new(Default::default());
//         let target = traverse_plan::<PolarsTarget>(&p, &mut tctx);

//         let msg = match target {
//             Err(PlanErr::Plan(ref s)) => s,
//             _ => "",
//         };

//         assert_eq!(msg, "joins are not supported on worker side");
//     }

//     #[test]
//     // We don't support worker-side joins!
//     fn test_plan_with_remote_join() {
//         let p = make_block_join_remote_example();
//         let mut tctx = TraversalContext::new(Default::default());
//         let target = traverse_plan::<PolarsTarget>(&p, &mut tctx);

//         let msg = match target {
//             Err(PlanErr::Plan(ref s)) => s,
//             _ => "",
//         };

//         assert_eq!(msg, "joins are not supported on worker side");
//     }

//     fn produce_block() -> DataFrame {
//         df![
//             "number" => [
//                 217710051,
//                 217710084,
//                 217710085,
//                 217710086,
//                 217710364,
//                 217710937,
//                 217710953,
//             ],
//             "hash" => [
//                 "3pxVPNMbfcdTEDuq6ZY7KroLX53y3hoJX4XkXFSo7LTY",
//                 "ACabnvdRdFYLutek6qjpjwkx2y6TBSnCMiFKhQF3SdSx",
//                 "5rcMrqfqra4eLe5Tcxf5beV8GFFQhWnX4kSmrjkeYQKY",
//                 "53aH29s69KerKrbuwRHBMCF2f5e37tD6v2SKMViYksQz",
//                 "7kuVRVvBCHnNkEBNcmwEAG2akARhiUDCgKWJRDmwwLXp",
//                 "5M2NziC9dYPKgGPqDhkrSvJCLrX43QJvDxJCQE2nUXE1",
//                 "6GHM1c9pLTSDbdutNGvuvub44pFP6s71R6CsAqvuhhMr",
//             ],
//             "parent_number" => [
//                 217710050,
//                 217710051,
//                 217710084,
//                 217710085,
//                 217710363,
//                 217710936,
//                 217710952,
//             ],
//             "parent_hash" => [
//                 "3pxVPNMbfcdTEDuq6ZY7KroLX53y3hoJX4XkXFSo7LTY",
//                 "ACabnvdRdFYLutek6qjpjwkx2y6TBSnCMiFKhQF3SdSx",
//                 "5rcMrqfqra4eLe5Tcxf5beV8GFFQhWnX4kSmrjkeYQKY",
//                 "53aH29s69KerKrbuwRHBMCF2f5e37tD6v2SKMViYksQz",
//                 "7kuVRVvBCHnNkEBNcmwEAG2akARhiUDCgKWJRDmwwLXp",
//                 "5M2NziC9dYPKgGPqDhkrSvJCLrX43QJvDxJCQE2nUXE1",
//                 "6GHM1c9pLTSDbdutNGvuvub44pFP6s71R6CsAqvuhhMr",
//             ],
//             "height" => [
//                 200000002,
//                 200000035,
//                 200000036,
//                 200000037,
//                 200000315,
//                 200000880,
//                 200000896,
//             ],
//             "timestamp" => [
//                 "2023-09-15 12:46:22",
//                 "2023-09-15 12:46:38",
//                 "2023-09-15 12:46:38",
//                 "2023-09-15 12:46:38",
//                 "2023-09-15 12:46:38",
//                 "2023-09-15 12:52:57",
//                 "2023-09-15 12:53:04",
//             ],
//         ]
//         .unwrap()
//     }
// }
