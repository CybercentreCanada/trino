-- database: trino; groups: insert; mutable_tables: datatype|created; tables: datatype
-- delimiter: |; ignoreOrder: true;
--!
insert into ${mutableTables.hive.datatype} select * from datatype union all select * from datatype;
select * from ${mutableTables.hive.datatype}
--!
12|12.25|String1|1999-01-08|1999-01-08 02:05:06|true|123.22|12345678901234567890.0123456789|
25|55.52|test|1952-01-05|1989-01-08 04:05:06|false|321.21|-12345678901234567890.0123456789|
964|0.245|Again|1936-02-08|2005-01-09 04:05:06|false|333.82|98765432109876543210.9876543210|
100|12.25|testing|1949-07-08|2002-01-07 01:05:06|true|-393.22|-98765432109876543210.9876543210|
100|99.8777|AGAIN|1987-04-09|2010-01-02 04:03:06|true|000.00|00000000000000000000.0000000000|
5252|12.25|sample|1987-04-09|2010-01-02 04:03:06|true|123.00|00000000000000000001.0000000000|
100|9.8777|STRING1|1923-04-08|2010-01-02 05:09:06|true|010.01|00000000000000000002.0000000000|
8996|98.8777|again|1987-04-09|2010-01-02 04:03:06|false|-000.01|99999999999999999999.9999999999|
100|12.8788|string1|1922-04-02|2010-01-02 02:05:06|true|999.99|-99999999999999999999.9999999999|
5748|67.87|sample|1987-04-06|2010-01-02 04:03:06|true|-999.99|00000000000000000000.0000000001|
5748|67.87|Sample|1987-04-06|2010-01-02 04:03:06|true|181.18|-00000000000000000000.0000000001|
5748|67.87|sample|1987-04-06|2010-01-02 04:03:06|true|181.18|12345678901234567890.0123456789|
5748|67.87|sample|1987-04-06|2010-01-02 04:03:06|true|181.18|12345678901234567890.0123456789|
5000|67.87|testing|null|2010-01-02 04:03:06|null|null|null|
6000|null|null|1987-04-06|null|true|null|null|
null|98.52|null|null|null|true|181.18|null|
12|12.25|String1|1999-01-08|1999-01-08 02:05:06|true|123.22|12345678901234567890.0123456789|
25|55.52|test|1952-01-05|1989-01-08 04:05:06|false|321.21|-12345678901234567890.0123456789|
964|0.245|Again|1936-02-08|2005-01-09 04:05:06|false|333.82|98765432109876543210.9876543210|
100|12.25|testing|1949-07-08|2002-01-07 01:05:06|true|-393.22|-98765432109876543210.9876543210|
100|99.8777|AGAIN|1987-04-09|2010-01-02 04:03:06|true|000.00|00000000000000000000.0000000000|
5252|12.25|sample|1987-04-09|2010-01-02 04:03:06|true|123.00|00000000000000000001.0000000000|
100|9.8777|STRING1|1923-04-08|2010-01-02 05:09:06|true|010.01|00000000000000000002.0000000000|
8996|98.8777|again|1987-04-09|2010-01-02 04:03:06|false|-000.01|99999999999999999999.9999999999|
100|12.8788|string1|1922-04-02|2010-01-02 02:05:06|true|999.99|-99999999999999999999.9999999999|
5748|67.87|sample|1987-04-06|2010-01-02 04:03:06|true|-999.99|00000000000000000000.0000000001|
5748|67.87|Sample|1987-04-06|2010-01-02 04:03:06|true|181.18|-00000000000000000000.0000000001|
5748|67.87|sample|1987-04-06|2010-01-02 04:03:06|true|181.18|12345678901234567890.0123456789|
5748|67.87|sample|1987-04-06|2010-01-02 04:03:06|true|181.18|12345678901234567890.0123456789|
5000|67.87|testing|null|2010-01-02 04:03:06|null|null|null|
6000|null|null|1987-04-06|null|true|null|null|
null|98.52|null|null|null|true|181.18|null|
