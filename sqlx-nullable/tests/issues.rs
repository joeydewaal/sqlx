use nullable::{NullableState, Source, SqlFlavour, Table};


#[test]
pub fn one() {
    let orders_table = Table::new("vote")
        .push_column("id", false)
        .push_column("user_id", false);

    let table = Table::new("user");
    let user_table = table.push_column("id", false);

    let source = Source::new(vec![user_table, orders_table]);

    let query = r#"SELECT user.id, (SELECT COUNT(vote.id) FROM vote WHERE vote.user_id = user.id) as votes FROM user WHERE user.id = ?
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Sqlite);
    let nullable = state.get_nullable(&["id", "votes"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false])
}

#[test]
pub fn issue_2796() {
    let foo_table = Table::new("foo")
        .push_column("id", false)
        .push_column("name", false);

    let baz_table = Table::new("baz")
        .push_column("id", false)
        .push_column("name", false);

    let bar_table = Table::new("bar")
        .push_column("id", false)
        .push_column("foo_id", false)
        .push_column("baz_id", true)
        .push_column("name", false);

    let source = Source::new(vec![foo_table, baz_table, bar_table]);

    let query = r#"
        SELECT
            foo.id,
            foo.name,
            bar.id AS "bar_id",
            bar.name AS "bar_name",
            baz.id AS "baz_id",
            baz.name AS "baz_name"
        FROM foo
        LEFT JOIN bar ON bar.foo_id = foo.id
        LEFT JOIN baz ON baz.id = bar.baz_id "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "name", "bar_id", "bar_name", "baz_id", "baz_name"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, true, true])
}

#[test]
pub fn issue_367() {
    let color_table = Table::new("colors")
        .push_column("color_uuid", false)
        .push_column("color", true);

    let objects_table = Table::new("objects")
        .push_column("object_uuid", false)
        .push_column("colors", true);

    let source = Source::new(vec![color_table, objects_table]);

    let query = r#"
    SELECT o.object_uuid, o.colors,
    c1.color_uuid as color1_uuid, c1.color as color1_color,
    c2.color_uuid as color2_uuid, c2.color as color2_color
    FROM objects o
    LEFT JOIN colors c1 ON c1.color_uuid = o.colors[1]
    LEFT JOIN colors c2 ON c2.color_uuid = o.colors[2]
    "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "object_uuid",
        "colors",
        "color1_uuid",
        "color1_color",
        "color2_uuid",
        "color2_color",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, true, true, true, true, true])
}

#[test]
pub fn issue_367_2() {
    let color_table = Table::new("colors")
        .push_column("color_uuid", false)
        .push_column("color", true);

    let objects_table = Table::new("objects")
        .push_column("object_uuid", false)
        .push_column("colors", false);

    let source = Source::new(vec![color_table, objects_table]);

    let query = r#"
    SELECT o.object_uuid, o.colors,
    c1.color_uuid as color1_uuid, c1.color as color1_color,
    c2.color_uuid as color2_uuid, c2.color as color2_color
    FROM objects o
    LEFT JOIN colors c1 ON c1.color_uuid = o.colors[1]
    LEFT JOIN colors c2 ON c2.color_uuid = o.colors[2]
    "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "object_uuid",
        "colors",
        "color1_uuid",
        "color1_color",
        "color2_uuid",
        "color2_color",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, true, true])
}

#[test]
pub fn sqlx_issue_3202() {
    let user_table = Table::new("songs")
        .push_column("id", false)
        .push_column("artist", false)
        .push_column("title", false);

    let pets_table = Table::new("top50")
        .push_column("id", false)
        .push_column("year", false)
        .push_column("week", false)
        .push_column("song_id", false)
        .push_column("position", false);

    let source = Source::new(vec![user_table, pets_table]);

    let query = r#"
        SELECT
            this_week.week as cur_week,
            this_week.position as cur_position,
            prev_week.position as prev_position,
            (prev_week.position - this_week.position) as delta,
            songs.artist as artist,
            songs.title as title
        FROM
            top50 AS this_week
        INNER JOIN
            songs ON songs.id=this_week.song_id
        LEFT OUTER JOIN
            top50 as prev_week ON prev_week.song_id=this_week.song_id AND prev_week.week = this_week.week - 1
        WHERE this_week.week = 15
        ORDER BY this_week.week, this_week.position
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "cur_week",
        "cur_position",
        "prev_position",
        "delta",
        "artist",
        "title",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, false, false])
}

#[test]
pub fn sqlx_issue_3408() {
    let department_table = Table::new("department")
        .push_column("id", false)
        .push_column("name", false);

    let employee_table = Table::new("employee")
        .push_column("name", false)
        .push_column("department_id", true);

    let source = Source::new(vec![department_table, employee_table]);

    let query = r#"
       select
            employee.name as employee_name,
            department.name as department_name
       from employee
       left join
            department
       on
            employee.department_id = department.id
            and employee.name = $1
 "#;

    let mut state = NullableState::new(query, source.clone(), SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["employee_name", "department_name"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, true]);

    let query = r#"
        select
            employee.name as employee_name,
            department.name as department_name
        from employee
        left join
            department
            on employee.department_id = department.id
        where employee.name = $1
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["employee_name", "department_name"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, true]);
}
