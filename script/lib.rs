use polars::prelude;

fn polars_test() {
    // 从 Vec, 切片和数组, series 可以携带名称
    let s = Series::new("from vec", vec![1, 2, 3]);
    let s = Series::new("from slice", &[true, false, true]);
    let s = Series::new("from array", ["rookie", "long", "lin"]);

    // 空dataframe
    let df = DataFrame::default();
    assert!(df.is_empty());

    // 从多个series创建
    let s1 = Series::new("Fruit", &["Apple", "Apple", "Pear"]);
    let s2 = Series::new("Color", &["Red", "Yellow", "Green"]);
    let df: Result<DataFrame> = DataFrame::new(vec![s1, s2]);

    // 使用 df! 宏, 注意, 返回类型为Result<DataFrame>
    let df: Result<DataFrame> = df!("Fruit" => &["Apple", "Apple", "Pear"],
                                "Color" => &["Red", "Yellow", "Green"]);
    // 查看表头
    let countries: DataFrame =
        df!(
            "Rank by GDP (2021)" => &[1, 2, 3, 4, 5],
            "Continent" => &["North America", "Asia", "Asia", "Europe", "Europe"],
            "Country" => &["United States", "China", "Japan", "Germany", "United Kingdom"],
            "Capital" => &["Washington", "Beijing", "Tokyo", "Berlin", "London"]
        )?;
    assert_eq!(countries.shape(), (5, 4));
    println!("{}", countries.head(Some(3)));

    // 获取列名
    let df: DataFrame = df!("Language" => &["Rust", "Python"],
                            "Designer" => &["Graydon Hoare", "Guido van Rossum"])?;
    assert_eq!(df.get_column_names(), &["Language", "Designer"]);

    // 统计汇总方法
    let df1: DataFrame = df!("D1" => &[1, 3, 1, 5, 6],
                        "D2" => &[3, 2, 3, 5, 3])?;
    
    let df1: DataFrame = df!("D1" => &[1, 3, 1, 5, 6],
                            "D2" => &[3, 2, 3, 5, 3])?;
    let df2 = df1
       .lazy()
       .select(&[
           col("D1").count().alias("total"),
           col("D1").filter(col("D1").gt(lit(3))).count().alias("D1 > 3"),
       ])
       .collect()
       .unwrap();
    println!("{}", df2);
    /*
    其他方法
    其他统计学方法名称与 pandas 中同名或类似

    标准差 std
    分位数 quantile
    最大值 max
    最小值 min
    中位数 median
    */

    // 转置
    let df1: DataFrame = df!("D1" => &[1, 3, 1, 5, 6],
    "D2" => &[3, 2, 3, 5, 3])?;
    println!("{}", df1.transpose()?);

    // 排序
    let df1: DataFrame = df!("D1" => &[1, 3, 1, 5, 6],
                         "D2" => &[3, 2, 3, 5, 3])?;
    println!("{}", df1.sort(["D1"], vec![true])?);
    
    // 获取列
    let df1: DataFrame = df!("D1" => &[1, 3, 1, 5, 6],
                         "D2" => &[3, 2, 3, 5, 3])?;
    println!("{}", df1[1]);
    println!("{}", df1["D1"]);

    // 获取切片
    let df1: DataFrame = df!("D1" => &[1, 3, 1, 5, 6],
                        "D2" => &[3, 2, 3, 5, 3])?;
    println!("{}", df1.slice(2, 3));

    // 获取子区域
    let df1: DataFrame = df!(
        "D1" => &[1, 3, 1, 5, 6],
        "D2" => &[3, 2, 3, 5, 3],
        "D3" => &[9, 7, 5, 2, 4]
    )?;
    let df2 = df1
        .lazy()
        .select([cols(["D1", "D3"]).slice(2, 3)])
        .collect()?;
    println!("{}", df2);

    // 获取某个值
    let df1: DataFrame = df!(
        "D1" => &[1, 3, 1, 5, 6],
        "D2" => &[3, 2, 3, 5, 3],
        "D3" => &[9, 7, 5, 2, 4]
    )?;
    println!("{}", df1["D1"].get(0));
    
    // 布尔索引
    let df1: DataFrame = df!(
        "D1" => &[1, 3, 1, 5, 6],
        "D2" => &[3, 2, 3, 5, 3],
        "D3" => &[9, 7, 5, 2, 4]
    )?;
    let df2 = df1.lazy().filter(col("D1").gt(3)).collect()?;
    println!("{}", df2);

    // 添加新列
    let df1: DataFrame = df!(
        "D1" => &[1, 3, 1, 5, 6],
        "D2" => &[3, 2, 3, 5, 3],
        "D3" => &[9, 7, 5, 2, 4]
    )?;
    let df2 = df1
        .lazy()
        .with_column(
            when(col("D1").gt(3))
                .then(lit(true))
                .otherwise(lit(false))
                .alias("D1 > 3"),
        )
        .collect()?;
    println!("{}", df2);

    // 修改数据 replace + set_at_idx
    let mut df1: DataFrame = df!(
        "D1" => &[1, 3, 1, 5, 6],
        "D2" => &[3, 2, 3, 5, 3],
        "D3" => &[9, 7, 5, 2, 4]
    )?;
    let new_d1 = df1["D1"]
        .i32()
        .and_then(|s| s.set_at_idx(vec![0], Some(100)))?;
    let df2 = df1.replace("D1", new_d1)?;
    println!("{}", df2);

    // 处理缺失值
    let df1: DataFrame = df!(
        "D1" => &[1, 3, 1, 5, 6],
        "D2" => &[3, 2, 3, 5, 3],
        "D3" => &[Some(9), Some(7), Some(5), Some(2), None]
    )?;
    println!("{}", df1.null_count());
    println!("{}", df1["D3"].is_null().into_series());
    // 过滤掉包含缺失值的行
    let df1: DataFrame = df!(
        "D1" => &[1, 2, 3, 4, 5],
        "D2" => &[Some(3), Some(2), None, Some(5), Some(3)],
        "D3" => &[Some(9), Some(7), Some(5), Some(2), None]
    )?;
    let df2 = df1.lazy().filter(all().is_not_null()).collect()?;

    // 只针对某列过滤
    let df1: DataFrame = df!(
        "D1" => &[1, 2, 3, 4, 5],
        "D2" => &[Some(3), Some(2), None, Some(5), Some(3)],
        "D3" => &[Some(9), Some(7), Some(5), Some(2), None]
    )?;
    let df2 = df1.lazy().filter(col("D3").is_not_null()).collect()?;
    println!("{}", df2);

    // 填充缺失值
    let df1: DataFrame = df!(
        "D1" => &[1, 2, 3, 4, 6],
        "D2" => &[3, 2, 8, 5, 3],
        "D3" => &[Some(9), Some(7), None, Some(2), Some(4)]
      )?;
      // 对全部 null 全部更换为 200
      let df2 = df1.clone().lazy().fill_null(lit(200)).collect()?;
      println!("对全部 null 全部更换为 200\n{}", df2);
      
      // 以下一个值填充
      let df2 = df1
        .clone()
        .lazy()
        .with_column(col("D3").backward_fill(None))
        .collect()?;
      println!("以下一个值填充\n{}", df2);
      
      // 以前一个值填充
      let df2 = df1
        .clone()
        .lazy()
        .with_column(col("D3").forward_fill(None))
        .collect()?;
      println!("以前一个值填充\n{}", df2);
      
      // 插值
      let df2 = df1
        .clone()
        .lazy()
        .with_column(col("D3").interpolate())
        .collect()?;
      println!("插值\n{}", df2);
}