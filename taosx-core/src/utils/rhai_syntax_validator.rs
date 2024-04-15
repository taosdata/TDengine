use rhai::{Engine, EvalAltResult, Scope};
use regex::Regex;

/**
 * 数学表达式.
 * 固定使用 f64 统一校验，如果需要其他类型，可以自行修改.
 */
pub fn check_math_expression(field_name: &str, expression: &str) -> Result<(), String> {
    let mut scope = Scope::new();
    scope.push(field_name, 100.0_f64);
    let engine = Engine::new();
    match engine.eval_expression_with_scope::<f64>(&mut scope, expression) {
        Ok(_) => Ok(()),
        Err(mut e) => {
           e.clear_position();
           Err(e.to_string())
        },
    } 
}

// async fn check_bool_expression(params: HashMap<String, String>, expression: &str) -> Result<(), String> {
//     let mut scope = Scope::new();
//     scope.push(field_name, true);
    
//     let engine = Engine::new();
//     match engine.eval_expression_with_scope::<f64>(&mut scope, expression) {
//         Ok(_) => Ok(()),
//         Err(e) => Err(e.to_string()),
//     } 
// }


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_math_expression() {
        let field_name = "x";
        let expression = "x + 1";
        let result = check_math_expression(field_name, expression);
        assert_eq!(result, Ok(()));

        let expression = "x.log() + 1";
        let result = check_math_expression(field_name, expression);
        assert_eq!(result, Ok(()));

        let expression = "x.lgs() + 1";
        let result = check_math_expression(field_name, expression);
        println!("{:?}", result);
        assert!(result.is_err());

        let expression = "x + 1 + y";
        let result = check_math_expression(field_name, expression);
        println!("{:?}", result);
        assert!(result.is_err());
    }

    // #[tokio::test]
    // async fn test_check_bool_expression() {
    //     let params = HashMap::new();
    //     let expression = "true";
    //     let result = check_bool_expression(params, expression).await;
    //     assert_eq!(result, Ok(()));
    // }
}
