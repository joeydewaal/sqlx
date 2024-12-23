use nullable::{NullableState, Source, SqlFlavour, Table};

#[test]
pub fn func1() {
    let orders_table = Table::new("agenda")
        .push_column("agenda_id", false)
        .push_column("startdate", false)
        .push_column("user_id", false);

    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", true);

    let source = Source::new(vec![user_table, orders_table]);

    let query = r#"
        select
            a.agenda_id,
            a.startdate,
            u.user_id,
            array_agg((u.user_id, u.name)),
            array_remove(array_agg((u.user_id, u.name)), null)
        from
            agenda a
        inner join
            users u on a.user_id = u.user_id
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable =
        state.get_nullable(&["agenda_id", "startdate", "user_id", "?column?", "?column?"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, false, false, false])
}

#[test]
pub fn func2() {
    let source = Source::empty();

    let query = r#"
        select current_timestamp
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["current_timestamp"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}
