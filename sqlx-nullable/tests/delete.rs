use nullable::{NullableState, Source, SqlFlavour, Table};

#[test]
pub fn delete_1() {
    let persons_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", false);

    let source = Source::new(vec![persons_table]);

    let query = r#"
        delete from users where 1 = 2 returning *
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "username", "emailadres", "pet_id"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, false])
}
