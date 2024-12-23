use nullable::{NullableState, Source, SqlFlavour};

#[test]
pub fn nested() {
    let source = Source::empty();

    let query = r#"
select 1 as test, 2 as test1, (select 1)
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["test", "test1", "?column?"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, false])
}

#[test]
pub fn nested_2() {
    let source = Source::empty();

    let query = r#"
select test.id from (select 1 as id) as test
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn nested_3() {
    let source = Source::empty();

    let query = r#"
select id from (select 1 as id)
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn nested_4() {
    let source = Source::empty();

    let query = r#"
select * from  unnest(ARRAY[1, 2, 3])
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["unnest"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn nested_5() {
    let source = Source::empty();

    let query = r#"
select * from  unnest(ARRAY[1, 2, 3])
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["unnest"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}
