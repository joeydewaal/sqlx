use nullable::{NullableState, Source, SqlFlavour};

#[test]
pub fn create_1() {
    let source = Source::empty();

    let query = r#"
        create table users(id serial);
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[]);
    println!("{:?}", nullable);
    assert!(nullable == [])
}
