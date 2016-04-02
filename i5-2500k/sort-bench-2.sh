DB=test
ROWS=10000000
WORK_MEM='1GB'

function log {
	echo `date +%s` [`date +'%Y-%m-%d %H:%M:%S'`] $1
}

function create_tables {

	psql $DB > /dev/null  <<EOF
-- tables with master data (running generate_series only once)
CREATE TABLE data_int (a INT);
INSERT INTO  data_int SELECT i FROM generate_series(1, $ROWS) s(i);

CREATE TABLE data_float (a INT);
INSERT INTO  data_float SELECT 100000 * random() FROM generate_series(1, $ROWS) s(i);

-- tables used for the actual testing
CREATE TABLE int_test         (a INT);
CREATE TABLE int_test_padding (a INT, b TEXT);

CREATE TABLE text_test         (a TEXT);
CREATE TABLE text_test_padding (a TEXT, b TEXT);

CREATE TABLE numeric_test         (a NUMERIC);
CREATE TABLE numeric_test_padding (a NUMERIC, b TEXT);
EOF

}

function truncate_tables {

	log "truncating tables"

	psql $DB > /dev/null  <<EOF
TRUNCATE TABLE int_test;
TRUNCATE TABLE int_test_padding;

TRUNCATE TABLE text_test;
TRUNCATE TABLE text_test_padding;

TRUNCATE TABLE numeric_test;
TRUNCATE TABLE numeric_test_padding;
EOF

}

function show_table_sizes {

        psql $DB -c '\d+' > $1.log

}

function vacuum_analyze {

	log "analyzing"

	psql $DB > /dev/null  <<EOF
VACUUM ANALYZE;
EOF

}

# unique data

function load_unique_sorted {

	truncate_tables

	log "loading unique tables / sorted"

	psql $DB > /dev/null  <<EOF
SET work_mem = '$WORK_MEM';

INSERT INTO int_test SELECT a FROM data_int;
INSERT INTO int_test_padding SELECT a, repeat(md5(a::text),10) FROM data_int;

INSERT INTO text_test SELECT md5(a::text) FROM data_int ORDER BY md5(a::text);
INSERT INTO text_test_padding SELECT md5(a::text), repeat(md5((a+1)::text),10) FROM data_int ORDER BY md5(a::text);

INSERT INTO numeric_test SELECT a FROM data_float ORDER BY a;
INSERT INTO numeric_test_padding SELECT a, repeat(md5(a::text),10) FROM data_float ORDER BY a;
EOF

	vacuum_analyze

	show_table_sizes "unique_sorted"

}

function load_unique_random {

	truncate_tables

	log "loading unique tables / random"

	psql $DB > /dev/null  <<EOF
SET work_mem = '$WORK_MEM';

-- this needs randomization
INSERT INTO int_test SELECT a FROM data_int ORDER BY random();
INSERT INTO int_test_padding SELECT a, repeat(md5(a::text),10) FROM data_int ORDER BY random();

-- these already are random
INSERT INTO text_test SELECT md5(a::text) FROM data_int;
INSERT INTO text_test_padding SELECT md5(a::text), repeat(md5((a+1)::text),10) FROM data_int;

INSERT INTO numeric_test SELECT a FROM data_float;
INSERT INTO numeric_test_padding SELECT a, repeat(md5(a::text),10) FROM data_float;
EOF

	vacuum_analyze

	show_table_sizes "unique_random"

}

function load_unique_almost_asc {

	truncate_tables

	log "loading unique tables / almost sorted"

	psql $DB > /dev/null  <<EOF
SET work_mem = '$WORK_MEM';

INSERT INTO int_test SELECT a FROM (
    SELECT a, rank() OVER (ORDER BY a) AS r FROM data_int
) foo ORDER BY r + (100 * random());

INSERT INTO int_test_padding SELECT a, repeat(b,10) FROM (
    SELECT a, md5(a::text) AS b, rank() OVER (ORDER BY a) AS r FROM data_int
) foo ORDER BY r + (100 * random());

INSERT INTO text_test SELECT a FROM (
    SELECT md5(a::text) AS a, rank() OVER (ORDER BY md5(a::text)) AS r FROM data_int
) foo ORDER BY r + (100 * random());

INSERT INTO text_test_padding SELECT a, repeat(b,10) FROM (
    SELECT md5(a::text) AS a, md5((a+1)::text) AS b, rank() OVER (ORDER BY md5(a::text)) AS r FROM data_int
) foo ORDER BY r + (100 * random());

INSERT INTO numeric_test SELECT a FROM (
    SELECT a, rank() OVER (ORDER BY a) AS r FROM data_float
) foo ORDER BY r + (100 * random());

INSERT INTO numeric_test_padding SELECT a, repeat(b,10) FROM (
    SELECT a, md5(a::text) AS b, rank() OVER (ORDER BY a) AS r FROM data_float
) foo ORDER BY r + (100 * random());
EOF

	vacuum_analyze

	show_table_sizes "unique_almost_asc"

}

# high cardinality (10% of table size)

function load_high_cardinality_sorted {

	truncate_tables

	log "loading high cardinality tables / sorted"

	psql $DB > /dev/null  <<EOF
SET work_mem = '$WORK_MEM';

INSERT INTO int_test SELECT a/10 FROM data_int;
INSERT INTO int_test_padding SELECT a/10, repeat(md5(a::text),10) FROM data_int;

INSERT INTO text_test SELECT md5((a/10)::text) FROM data_int ORDER BY md5((a/10)::text);
INSERT INTO text_test_padding SELECT md5((a/10)::text), repeat(md5((a+1)::text),10) FROM data_int ORDER BY md5(a::text);

INSERT INTO numeric_test SELECT hashint4(a/10)::numeric/100 FROM data_int ORDER BY a;
INSERT INTO numeric_test_padding SELECT hashint4(a/10)::numeric/100, repeat(md5(a::text),10) FROM data_int ORDER BY a;
EOF

	vacuum_analyze

	show_table_sizes "high_cardinality_sorted"

}

function load_high_cardinality_random {

	truncate_tables

	log "loading high cardinality tables / random"

	psql $DB > /dev/null  <<EOF

-- this needs randomization
INSERT INTO int_test SELECT a/10 FROM data_int ORDER BY random();
INSERT INTO int_test_padding SELECT a/10, md5(a::text) FROM data_int ORDER BY random();

-- these already are random
INSERT INTO text_test SELECT md5((a/10)::text) FROM data_int;
INSERT INTO text_test_padding SELECT md5((a/10)::text), md5((a+1)::text) FROM data_int;

INSERT INTO numeric_test SELECT hashint4(a/10)::numeric/100 FROM data_int;
INSERT INTO numeric_test_padding SELECT hashint4(a/10)::numeric/100, md5(a::text) FROM data_int;
EOF

	vacuum_analyze

	show_table_sizes "high_cardinality_random"

}

function load_high_cardinality_almost_asc {

	truncate_tables

	log "loading high cardinality tables / almost sorted"

	psql $DB > /dev/null  <<EOF
SET work_mem = '$WORK_MEM';

INSERT INTO int_test SELECT a FROM (
    SELECT (a/10) AS a, rank() OVER (ORDER BY (a/10)) AS r FROM data_int
) foo ORDER BY r + (100 * random());

INSERT INTO int_test_padding SELECT a, repeat(b,10) FROM (
    SELECT (a/10) AS a, md5(a::text) AS b, rank() OVER (ORDER BY (a/10)) AS r FROM data_int
) foo ORDER BY r + (100 * random());
 
INSERT INTO text_test SELECT a FROM (
    SELECT md5((a/10)::text) AS a, rank() OVER (ORDER BY md5((a/10)::text)) AS r FROM data_int
) foo ORDER BY r + (100 * random());

INSERT INTO text_test_padding SELECT a, repeat(b,10) FROM (
    SELECT md5((a/10)::text) AS a, md5((a/10+1)::text) AS b, rank() OVER (ORDER BY md5((a/10)::text)) AS r FROM data_int
) foo ORDER BY r + (100 * random());

INSERT INTO numeric_test SELECT a FROM (
    SELECT ((a/10)::numeric/1000) AS a, rank() OVER (ORDER BY (a/10)::numeric) AS r FROM data_int
) foo ORDER BY r + (100 * random());

INSERT INTO numeric_test_padding SELECT a, repeat(b,10) FROM (
    SELECT ((a/10)::numeric/1000) AS a, md5((a/10)::text) AS b, rank() OVER (ORDER BY (a/10)) AS r FROM data_int
) foo ORDER BY r + (100 * random());
EOF

	vacuum_analyze

	show_table_sizes "high_cardinality_almost_asc"

}

# low cardinality (1% of table size)

function load_low_cardinality_sorted {

	truncate_tables

	log "loading low cardinality tables / sorted"

	psql $DB > /dev/null  <<EOF
SET work_mem = '$WORK_MEM';

INSERT INTO int_test SELECT a/10000 FROM data_int;
INSERT INTO int_test_padding SELECT a/10000, repeat(md5(a::text),10) FROM data_int;

INSERT INTO text_test SELECT md5((a/10000)::text) FROM data_int ORDER BY md5((a/100)::text);
INSERT INTO text_test_padding SELECT md5((a/10000)::text), repeat(md5((a+1)::text),10) FROM data_int ORDER BY md5(a::text);

INSERT INTO numeric_test SELECT hashint4(a/10000)::numeric/100 FROM data_int ORDER BY a;
INSERT INTO numeric_test_padding SELECT hashint4(a/10000)::numeric/100, repeat(md5(a::text),10) FROM data_int ORDER BY a;
EOF

	vacuum_analyze

	show_table_sizes "low_cardinality_sorted"

}

function load_low_cardinality_random {

	truncate_tables

	log "loading low cardinality tables / random"

	psql $DB > /dev/null  <<EOF
SET work_mem = '$WORK_MEM';

-- this needs randomization
INSERT INTO int_test SELECT a/10000 FROM data_int ORDER BY random();
INSERT INTO int_test_padding SELECT a/10000, md5(a::text) FROM data_int ORDER BY random();

-- these already are random
INSERT INTO text_test SELECT md5((a/10000)::text) FROM data_int;
INSERT INTO text_test_padding SELECT md5((a/10000)::text), md5((a+1)::text) FROM data_int;

INSERT INTO numeric_test SELECT hashint4(a/10000)::numeric/100 FROM data_int;
INSERT INTO numeric_test_padding SELECT hashint4(a/10000)::numeric/100, md5(a::text) FROM data_int;
EOF

	vacuum_analyze

	show_table_sizes "low_cardinality_random"

}

function load_low_cardinality_almost_asc {

	truncate_tables

	log "loading low cardinality tables / almost sorted"

	psql $DB > /dev/null  <<EOF
SET work_mem = '$WORK_MEM';

INSERT INTO int_test SELECT a FROM (
    SELECT (a/10000) AS a, rank() OVER (ORDER BY (a/10000)) AS r FROM data_int
) foo ORDER BY r + (100 * random());

INSERT INTO int_test_padding SELECT a, repeat(b,10) FROM (
    SELECT (a/10000) AS a, md5(a::text) AS b, rank() OVER (ORDER BY (a/10000)) AS r FROM data_int
) foo ORDER BY r + (100 * random());
 
INSERT INTO text_test SELECT a FROM (
    SELECT md5((a/10000)::text) AS a, rank() OVER (ORDER BY md5((a/10000)::text)) AS r FROM data_int
) foo ORDER BY r + (100 * random());

INSERT INTO text_test_padding SELECT a, repeat(b,10) FROM (
    SELECT md5((a/10000)::text) AS a, md5((a/10000+1)::text) AS b, rank() OVER (ORDER BY md5((a/10000)::text)) AS r FROM data_int
) foo ORDER BY r + (100 * random());

INSERT INTO numeric_test SELECT a FROM (
    SELECT ((a/10000)::numeric/100) AS a, rank() OVER (ORDER BY (a/10000)::numeric) AS r FROM data_int
) foo ORDER BY r + (100 * random());

INSERT INTO numeric_test_padding SELECT a, repeat(b,10) FROM (
    SELECT ((a/10000)::numeric/100) AS a, md5((a/10000)::text) AS b, rank() OVER (ORDER BY (a/10000)) AS r FROM data_int
) foo ORDER BY r + (100 * random());
EOF

	vacuum_analyze

	show_table_sizes "low_cardinality_almost_asc"

}

function run_query {

	times=""

	for i in `seq 1 5`; do

		/usr/bin/time -f '%e' -o 'query.time' psql $DB > /dev/null <<EOF
\pset pager off
\o /dev/null
SET work_mem='$2';
SET trace_sort='off';
SELECT * FROM ($3 OFFSET 1e10) ff;
EOF

		x=`cat query.time`
		times="$times $x"

	done

	echo `date +%s` $1 $2 "'$3'" $times

	psql $DB > /dev/null 2>&1 <<EOF
SET work_mem='$2';
SET trace_sort='on';
SELECT * FROM ($3 OFFSET 1e10) ff;
EOF

}

function run_index {

        times=""

        for i in `seq 1 5`; do

                /usr/bin/time -f '%e' -o 'query.time' psql $DB > /dev/null <<EOF
SET maintenance_work_mem='$2';
SET trace_sort='off';
$3
EOF

                x=`cat query.time`
                times="$times $x"

        done

	echo `date +%s` $1 $2 "'$3'" $times

	psql $DB > /dev/null 2>&1 <<EOF
SET maintenance_work_mem='$2';
SET trace_sort='on';
$3
EOF

}

function run_queries {

	for wm in '8MB' '32MB' '128MB' '512MB' '1GB'; do

		run_query $1 $wm 'SELECT * FROM int_test ORDER BY a'
		run_query $1 $wm 'SELECT * FROM int_test_padding ORDER BY a'

		run_query $1 $wm 'SELECT * FROM int_test ORDER BY a DESC'
		run_query $1 $wm 'SELECT * FROM int_test_padding ORDER BY a DESC'

		run_query $1 $wm 'SELECT COUNT(DISTINCT a) FROM int_test'
		run_query $1 $wm 'SELECT COUNT(DISTINCT a) FROM int_test_padding'

		run_query $1 $wm 'SELECT a FROM int_test UNION SELECT a FROM int_test_padding'

		run_index $1 $wm 'CREATE INDEX x ON int_test (a); DROP INDEX x'
		run_index $1 $wm 'CREATE INDEX x ON int_test_padding (a); DROP INDEX x'

		run_query $1 $wm 'SELECT * FROM text_test ORDER BY a'
		run_query $1 $wm 'SELECT * FROM text_test_padding ORDER BY a'

		run_query $1 $wm 'SELECT * FROM text_test ORDER BY a DESC'
		run_query $1 $wm 'SELECT * FROM text_test_padding ORDER BY a DESC'

		run_query $1 $wm 'SELECT COUNT(DISTINCT a) FROM text_test'
		run_query $1 $wm 'SELECT COUNT(DISTINCT a) FROM text_test_padding'

		run_query $1 $wm 'SELECT a FROM text_test UNION SELECT a FROM text_test_padding'

		run_index $1 $wm 'CREATE INDEX x ON text_test (a); DROP INDEX x'
		run_index $1 $wm 'CREATE INDEX x ON text_test_padding (a); DROP INDEX x'

		run_query $1 $wm 'SELECT * FROM numeric_test ORDER BY a'
		run_query $1 $wm 'SELECT * FROM numeric_test_padding ORDER BY a'

		run_query $1 $wm 'SELECT * FROM numeric_test ORDER BY a DESC'
		run_query $1 $wm 'SELECT * FROM numeric_test_padding ORDER BY a DESC'

		run_query $1 $wm 'SELECT COUNT(DISTINCT a) FROM numeric_test'
		run_query $1 $wm 'SELECT COUNT(DISTINCT a) FROM numeric_test_padding'

		run_query $1 $wm 'SELECT a FROM numeric_test UNION SELECT a FROM numeric_test_padding'

		run_index $1 $wm 'CREATE INDEX x ON numeric_test (a); DROP INDEX x'
		run_index $1 $wm 'CREATE INDEX x ON numeric_test_padding (a); DROP INDEX x'

	done

}

dropdb $DB
createdb $DB

create_tables

load_unique_sorted

run_queries "unique_sorted"

load_unique_random

run_queries "unique_random"

load_unique_almost_asc

run_queries "unique_almost_asc"

load_low_cardinality_sorted

run_queries "low_cardinality_sorted"

load_low_cardinality_random

run_queries "low_cardinality_random"

load_low_cardinality_almost_asc

run_queries "low_cardinality_almost_asc"

load_high_cardinality_sorted

run_queries "high_cardinality_sorted"

load_high_cardinality_random

run_queries "high_cardinality_random"

load_high_cardinality_almost_asc

run_queries "high_cardinality_almost_asc"

