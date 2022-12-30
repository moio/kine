package pgsql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/logstructured"
	"github.com/k3s-io/kine/pkg/logstructured/sqllog"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/k3s-io/kine/pkg/util"
	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

const (
	defaultDSN = "postgres://postgres:postgres@localhost/"
)

var (
	schema = []string{
		`CREATE TABLE IF NOT EXISTS kine
 			(
 				id SERIAL PRIMARY KEY,
				name VARCHAR(630),
				created INTEGER,
				deleted INTEGER,
 				create_revision INTEGER,
 				prev_revision INTEGER,
 				lease INTEGER,
 				value bytea,
 				old_value bytea
 			);`,
		`CREATE INDEX IF NOT EXISTS kine_name_index ON kine (name)`,
		`CREATE INDEX IF NOT EXISTS kine_name_id_index ON kine (name,id)`,
		`CREATE INDEX IF NOT EXISTS kine_id_deleted_index ON kine (id,deleted)`,
		`CREATE INDEX IF NOT EXISTS kine_prev_revision_index ON kine (prev_revision)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS kine_name_prev_revision_uindex ON kine (name, prev_revision)`,
		`CREATE INDEX IF NOT EXISTS kine_list_query_index on kine(name, id DESC) INCLUDE (deleted)`,
		`CREATE OR REPLACE FUNCTION list_from_kine (
			p_name_pattern VARCHAR,
			p_min_id INTEGER,
			p_min_key VARCHAR,
			p_max_id INTEGER,
			p_include_deleted BOOLEAN,
			p_result_limit INTEGER
		)
		RETURNS table (
			current_id INTEGER,
			compact_rev_id INTEGER,
			id INTEGER,
			name VARCHAR,
			created INTEGER,
			deleted INTEGER,
			create_revision INTEGER,
			prev_revision INTEGER,
			lease INTEGER,
			value BYTEA,
			old_value BYTEA
		)
		AS $$
			DECLARE
				current_id INTEGER;
				compact_rev_id INTEGER;
			BEGIN
				SELECT MAX(rkv.id) INTO current_id FROM kine AS rkv;
				SELECT MAX(crkv.prev_revision) INTO compact_rev_id FROM kine AS crkv WHERE crkv.name = 'compact_rev_key';
		
				RETURN QUERY
					SELECT DISTINCT ON (name)
						current_id,	compact_rev_id,
						kv.id AS theid, kv.name, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value
					FROM kine AS kv
					WHERE
						kv.name LIKE p_name_pattern
						AND (p_min_key IS NULL OR kv.name > p_min_key)
						AND kv.id <= p_max_id
						AND (kv.deleted = 0 OR p_include_deleted)
					ORDER BY kv.name, theid DESC
					LIMIT p_result_limit;
			END
		$$ LANGUAGE plpgsql;`,
	}
	createDB = "CREATE DATABASE %s;"
)

func New(ctx context.Context, dataSourceName string, tlsInfo tls.Config, connPoolConfig generic.ConnectionPoolConfig, metricsRegisterer prometheus.Registerer) (server.Backend, error) {
	parsedDSN, err := prepareDSN(dataSourceName, tlsInfo)
	if err != nil {
		return nil, err
	}

	if err := createDBIfNotExist(parsedDSN); err != nil {
		return nil, err
	}

	dialect, err := generic.Open(ctx, "postgres", parsedDSN, connPoolConfig, "$", true, metricsRegisterer)
	if err != nil {
		return nil, err
	}
	dialect.GetSizeSQL = `SELECT pg_total_relation_size('kine')`
	dialect.CompactSQL = `
		DELETE FROM kine AS kv
		USING	(
			SELECT kp.prev_revision AS id
			FROM kine AS kp
			WHERE
				kp.name != 'compact_rev_key' AND
				kp.prev_revision != 0 AND
				kp.id <= $1
			UNION
			SELECT kd.id AS id
			FROM kine AS kd
			WHERE
				kd.deleted != 0 AND
				kd.id <= $2
		) AS ks
		WHERE kv.id = ks.id`
	dialect.TranslateErr = func(err error) error {
		if err, ok := err.(*pq.Error); ok && err.Code == "23505" {
			return server.ErrKeyExists
		}
		return err
	}
	dialect.ErrCode = func(err error) string {
		if err == nil {
			return ""
		}
		if err, ok := err.(*pq.Error); ok {
			return string(err.Code)
		}
		return err.Error()
	}

	// integer ranges from -2147483648 to +2147483647

	dialect.GetCurrentSQL = q("SELECT * FROM list_from_kine(?, -2147483648, NULL, 2147483647, ?, NULL)")
	dialect.GetCurrentSQLLimited = q("SELECT * FROM list_from_kine(?, -2147483648, NULL, 2147483647, ?, ?)")
	dialect.ListRevisionStartSQL = q("SELECT * FROM list_from_kine(?, -2147483648, NULL, ?, ?, NULL)")
	dialect.ListRevisionStartSQLLimited = q("SELECT * FROM list_from_kine(?, -2147483648, NULL, ?, ?, ?)")
	dialect.GetRevisionAfterSQL = q("SELECT * FROM list_from_kine(?, ?, ?, ?, ?, NULL)")
	dialect.GetRevisionAfterSQLLimited = q("SELECT * FROM list_from_kine(?, ?, ?, ?, ?, ?)")

	dialect.CountSQL = q(fmt.Sprintf(`
			SELECT (SELECT MAX(rkv.id) AS id FROM kine AS rkv), COUNT(c.theid)
			FROM (
				SELECT DISTINCT ON (name)
					kv.id AS theid
				FROM kine AS kv
				WHERE
					kv.name LIKE ?
					AND (kv.deleted = 0 OR ?)
				ORDER BY kv.name, theid DESC
			) c`))

	if err := setup(dialect.DB); err != nil {
		return nil, err
	}

	dialect.Migrate(context.Background())
	return logstructured.New(sqllog.New(dialect)), nil
}

func setup(db *sql.DB) error {
	logrus.Infof("Configuring database table schema and indexes, this may take a moment...")

	for _, stmt := range schema {
		logrus.Tracef("SETUP EXEC : %v", util.Stripped(stmt))
		_, err := db.Exec(stmt)
		if err != nil {
			return err
		}
	}

	logrus.Infof("Database tables and indexes are up to date")
	return nil
}

func createDBIfNotExist(dataSourceName string) error {
	u, err := url.Parse(dataSourceName)
	if err != nil {
		return err
	}

	dbName := strings.SplitN(u.Path, "/", 2)[1]
	db, err := sql.Open("postgres", dataSourceName)
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.Ping()
	// check if database already exists
	if _, ok := err.(*pq.Error); !ok {
		return err
	}
	if err := err.(*pq.Error); err.Code != "42P04" {
		if err.Code != "3D000" {
			return err
		}
		// database doesn't exit, will try to create it
		u.Path = "/postgres"
		db, err := sql.Open("postgres", u.String())
		if err != nil {
			return err
		}
		defer db.Close()
		stmt := createDB + dbName + ";"
		logrus.Tracef("SETUP EXEC : %v", util.Stripped(stmt))
		_, err = db.Exec(stmt)
		if err != nil {
			return err
		}
	}
	return nil
}

func q(sql string) string {
	regex := regexp.MustCompile(`\?`)
	pref := "$"
	n := 0
	return regex.ReplaceAllStringFunc(sql, func(string) string {
		n++
		return pref + strconv.Itoa(n)
	})
}

func prepareDSN(dataSourceName string, tlsInfo tls.Config) (string, error) {
	if len(dataSourceName) == 0 {
		dataSourceName = defaultDSN
	} else {
		dataSourceName = "postgres://" + dataSourceName
	}
	u, err := url.Parse(dataSourceName)
	if err != nil {
		return "", err
	}
	if len(u.Path) == 0 || u.Path == "/" {
		u.Path = "/kubernetes"
	}

	queryMap, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", err
	}
	// set up tls dsn
	params := url.Values{}
	sslmode := ""
	if _, ok := queryMap["sslcert"]; tlsInfo.CertFile != "" && !ok {
		params.Add("sslcert", tlsInfo.CertFile)
		sslmode = "verify-full"
	}
	if _, ok := queryMap["sslkey"]; tlsInfo.KeyFile != "" && !ok {
		params.Add("sslkey", tlsInfo.KeyFile)
		sslmode = "verify-full"
	}
	if _, ok := queryMap["sslrootcert"]; tlsInfo.CAFile != "" && !ok {
		params.Add("sslrootcert", tlsInfo.CAFile)
		sslmode = "verify-full"
	}
	if _, ok := queryMap["sslmode"]; !ok && sslmode != "" {
		params.Add("sslmode", sslmode)
	}
	for k, v := range queryMap {
		params.Add(k, v[0])
	}
	u.RawQuery = params.Encode()
	return u.String(), nil
}
