use nullable::{NullableState, Source, SqlFlavour, Table};

#[test]
pub fn values() {
    let source = Source::empty();

    let query = r#"
        values (1, 2)
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?column?", "?column"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false])
}

#[test]
pub fn query_1() {
    let source = Source::empty();

    let query = r#"
        (select 1)
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?column?"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[ignore = "sqlparser does currently not support table commands"]
#[test]
pub fn table_1() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("name", false)
        .push_column("emailadres", true);
    let source = Source::new(vec![user_table]);

    let query = r#"
        TABLE users;
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "name", "emailadres"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true])
}

#[test]
pub fn compound_acces() {
    let source = Source::empty();

    let query = r#"
        SELECT (information_schema._pg_expandarray(array['i','i'])).n
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["n"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn compound_acces_2() {
    let source = Source::empty();

    let query = r#"
        SELECT (information_schema._pg_expandarray(array[NULL, NULL])).n
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["n"]);
    println!("{:?}", nullable);
    assert!(nullable == [true])
}

#[test]
pub fn in_list() {
    let source = Source::empty();

    let query = r#"
        SELECT 1 in (NULL, 2)
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?column?"]);
    println!("{:?}", nullable);
    assert!(nullable == [true])
}

#[test]
pub fn in_list_2() {
    let source = Source::empty();

    let query = r#"
        SELECT 1 in (1, 2)
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?column?"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}


#[test]
pub fn is_distinct_from() {
    let source = Source::empty();

    let query = r#"
        SELECT 1 is distinct from null
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?column?"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn in_subquery() {
    let source = Source::empty();

    let query = r#"
        SELECT 1 in (select null::int)
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?column?"]);
    println!("{:?}", nullable);
    assert!(nullable == [true])
}

#[test]
pub fn in_subquery_2() {
    let source = Source::empty();

    let query = r#"
        SELECT null in (select 1)
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?column?"]);
    println!("{:?}", nullable);
    assert!(nullable == [true])
}

#[test]
pub fn in_subquery_3() {
    let source = Source::empty();

    let query = r#"
        SELECT 1 in (select 1)
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?column?"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}
