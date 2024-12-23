use sqlparser::ast::{
    Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, ObjectName,
};

use crate::{context::Context, expr::visit_expr, nullable::NullableResult};

pub fn visit_func(func: &Function, context: &mut Context) -> anyhow::Result<NullableResult> {
    let function_name = func_name(&func.name);
    let f: Vec<_> = function_name.iter().map(|n| n.as_str()).collect();
    let inferred_nullable = match f[..] {
        ["count"] | ["current_user"] | ["now"] | ["random"] | ["version"] => Some(false),
        ["lower"]
        | ["upper"]
        | ["concat"]
        | ["length"]
        | ["abs"]
        | ["ceil"]
        | ["ceiling"]
        | ["floor"]
        | ["round"]
        | ["power"]
        | ["sum"]
        | ["avg"]
        | ["min"]
        | ["max"]
        | ["information_schema", "_pg_expandarray"] => {
            let nullables = args_nullables(&func.args, context)?;

            if nullables.len() > 0 && nullables.iter().all(|n| *n == Some(false)) {
                Some(false)
            } else {
                None
            }
        }
        ["coalesce"] => {
            let nullables = args_nullables(&func.args, context)?;

            if !nullables.is_empty() && nullables.iter().any(|n| *n == Some(false)) {
                Some(false)
            } else {
                None
            }
        }
        ["array_agg"] | ["array_remove"] => {
            let nullables = args_nullables(&func.args, context)?;

            if !nullables.is_empty() {
                Some(false)
            } else {
                None
            }
        }
        ["current_timestamp"] if args_nullables(&func.args, context)?.is_empty() => Some(false),
        ["generate_series"] => Some(false),
        _ => unimplemented!("{func:?}"),
    };

    Ok(NullableResult::unnamed(inferred_nullable))
}

fn args_nullables(
    args: &FunctionArguments,
    context: &mut Context,
) -> anyhow::Result<Vec<Option<bool>>> {
    match args {
        FunctionArguments::List(list) => arg_list_nullable(&list, context),
        FunctionArguments::None => Ok(Vec::new()),
        subquery => unimplemented!("{subquery:?}"),
    }
}

fn arg_list_nullable(
    arg_list: &FunctionArgumentList,
    context: &mut Context,
) -> anyhow::Result<Vec<Option<bool>>> {
    arg_list
        .args
        .iter()
        .map(|a| func_list_arg_nullable(a, context).map(|r| r.value))
        .collect()
}

fn func_list_arg_nullable(
    arg: &FunctionArg,
    context: &mut Context,
) -> anyhow::Result<NullableResult> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => visit_expr(expr, None, context),
        _ => unimplemented!(),
    }
}

fn func_name(obj: &ObjectName) -> Vec<String> {
    obj.0.iter().map(|i| i.value.to_lowercase()).collect()
}
