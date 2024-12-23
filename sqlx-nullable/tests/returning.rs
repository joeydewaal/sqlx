use nullable::{NullableState, Source, SqlFlavour, Table};

#[test]
pub fn returning_basic() {
    let orders_table = Table::new("users")
        .push_column("id", false)
        .push_column("name", false);
    let source = Source::new(vec![orders_table]);

    let query = r#"
        update users
        set id = 1
        returning
            id, name
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "name"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false])
}

#[test]
pub fn returning_basic_2() {
    let orders_table = Table::new("users")
        .push_column("id", false)
        .push_column("name", true);
    let source = Source::new(vec![orders_table]);

    let query = r#"
        update users
        set id = 1
        returning
            id, name
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "name"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, true])
}

#[test]
pub fn returning_basic_3() {
    let orders_table = Table::new("users")
        .push_column("id", false)
        .push_column("name", true);
    let source = Source::new(vec![orders_table]);

    let query = r#"
        update users
        set id = 1
        returning
            *
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "name"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, true])
}
