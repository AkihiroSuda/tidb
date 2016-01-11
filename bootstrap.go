// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tidb

import (
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
)

const (
	// CreateUserTable is the SQL statement creates User table in system db.
	CreateUserTable = `CREATE TABLE if not exists mysql.user (
		Host			CHAR(64),
		User			CHAR(16),
		Password		CHAR(41),
		Select_priv		ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Insert_priv		ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Update_priv		ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Delete_priv		ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Create_priv		ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Drop_priv		ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Grant_priv		ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Alter_priv		ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Show_db_priv		ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Execute_priv		ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Index_priv		ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Create_user_priv	ENUM('N','Y') NOT NULL  DEFAULT 'N',
		PRIMARY KEY (Host, User));`
	// CreateDBPrivTable is the SQL statement creates DB scope privilege table in system db.
	CreateDBPrivTable = `CREATE TABLE if not exists mysql.db (
		Host		CHAR(60),
		DB		CHAR(64),
		User		CHAR(16),
		Select_priv	ENUM('N','Y') Not Null  DEFAULT 'N',
		Insert_priv	ENUM('N','Y') Not Null  DEFAULT 'N',
		Update_priv	ENUM('N','Y') Not Null  DEFAULT 'N',
		Delete_priv	ENUM('N','Y') Not Null  DEFAULT 'N',
		Create_priv	ENUM('N','Y') Not Null  DEFAULT 'N',
		Drop_priv	ENUM('N','Y') Not Null  DEFAULT 'N',
		Grant_priv	ENUM('N','Y') Not Null  DEFAULT 'N',
		Index_priv	ENUM('N','Y') Not Null  DEFAULT 'N',
		Alter_priv	ENUM('N','Y') Not Null  DEFAULT 'N',
		Execute_priv	ENUM('N','Y') Not Null  DEFAULT 'N',
		PRIMARY KEY (Host, DB, User));`
	// CreateTablePrivTable is the SQL statement creates table scope privilege table in system db.
	CreateTablePrivTable = `CREATE TABLE if not exists mysql.tables_priv (
		Host		CHAR(60),
		DB		CHAR(64),
		User		CHAR(16),
		Table_name	CHAR(64),
		Grantor		CHAR(77),
		Timestamp	Timestamp DEFAULT CURRENT_TIMESTAMP,
		Table_priv	SET('Select','Insert','Update','Delete','Create','Drop','Grant', 'Index','Alter'),
		Column_priv	SET('Select','Insert','Update'),
		PRIMARY KEY (Host, DB, User, Table_name));`
	// CreateColumnPrivTable is the SQL statement creates column scope privilege table in system db.
	CreateColumnPrivTable = `CREATE TABLE if not exists mysql.columns_priv(
		Host		CHAR(60),
		DB		CHAR(64),
		User		CHAR(16),
		Table_name	CHAR(64),
		Column_name	CHAR(64),
		Timestamp	Timestamp DEFAULT CURRENT_TIMESTAMP,
		Column_priv	SET('Select','Insert','Update'),
		PRIMARY KEY (Host, DB, User, Table_name, Column_name));`
	// CreateGloablVariablesTable is the SQL statement creates global variable table in system db.
	// TODO: MySQL puts GLOBAL_VARIABLES table in INFORMATION_SCHEMA db.
	// INFORMATION_SCHEMA is a virtual db in TiDB. So we put this table in system db.
	// Maybe we will put it back to INFORMATION_SCHEMA.
	CreateGloablVariablesTable = `CREATE TABLE if not exists mysql.GLOBAL_VARIABLES(
		VARIABLE_NAME  VARCHAR(64) Not Null PRIMARY KEY,
		VARIABLE_VALUE VARCHAR(1024) DEFAULT Null);`
	// CreateProcTable is the SQL statement creates a dummy table in system db.
	// TODO: simplify & indent
	// TODO: what if.. insertion occurs?
	CreateProcTable = `CREATE TABLE if not exists mysql.proc (
  db char(64) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL DEFAULT '',
  name char(64) NOT NULL DEFAULT '',
  type enum('FUNCTION','PROCEDURE') NOT NULL,
  specific_name char(64) NOT NULL DEFAULT '',
  language enum('SQL') NOT NULL DEFAULT 'SQL',
  sql_data_access enum('CONTAINS_SQL','NO_SQL','READS_SQL_DATA','MODIFIES_SQL_DATA') NOT NULL DEFAULT 'CONTAINS_S
QL',
  is_deterministic enum('YES','NO') NOT NULL DEFAULT 'NO',
  security_type enum('INVOKER','DEFINER') NOT NULL DEFAULT 'DEFINER',
  param_list blob NOT NULL,
  returns longblob NOT NULL,
  body longblob NOT NULL,
  definer char(77) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL DEFAULT '',
  created timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  modified timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  sql_mode set('REAL_AS_FLOAT','PIPES_AS_CONCAT','ANSI_QUOTES','IGNORE_SPACE','NOT_USED','ONLY_FULL_GROUP_BY','NO_UNSIGNED_SUBTRACTION','NO_DIR_IN_CREATE','POSTGRESQL','ORACLE','MSSQL','DB2','MAXDB','NO_KEY_OPTIONS','NO_TABLE_OPTIONS','NO_FIELD_OPTIONS','MYSQL323','MYSQL40','ANSI','NO_AUTO_VALUE_ON_ZERO','NO_BACKSLASH_ESCAPES','STRICT_TRANS_TABLES','STRICT_ALL_TABLES','NO_ZERO_IN_DATE','NO_ZERO_DATE','INVALID_DATES','ERROR_FOR_DIVISION_BY_ZERO','TRADITIONAL','NO_AUTO_CREATE_USER','HIGH_NOT_PRECEDENCE','NO_ENGINE_SUBSTITUTION','PAD_CHAR_TO_FULL_LENGTH') NOT NULL DEFAULT '',
  comment text CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  character_set_client char(32) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
  collation_connection char(32) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
  db_collation char(32) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
  body_utf8 longblob,
  PRIMARY KEY (db,name,type));`
	CreateSomething1 = `
CREATE TABLE if not exists performance_schema.events_statements_current (
  THREAD_ID bigint(20) unsigned NOT NULL,
  EVENT_ID bigint(20) unsigned NOT NULL,
  END_EVENT_ID bigint(20) unsigned DEFAULT NULL,
  EVENT_NAME varchar(128) NOT NULL,
  SOURCE varchar(64) DEFAULT NULL,
  TIMER_START bigint(20) unsigned DEFAULT NULL,
  TIMER_END bigint(20) unsigned DEFAULT NULL,
  TIMER_WAIT bigint(20) unsigned DEFAULT NULL,
  LOCK_TIME bigint(20) unsigned NOT NULL,
  SQL_TEXT longtext,
  DIGEST varchar(32) DEFAULT NULL,
  DIGEST_TEXT longtext,
  CURRENT_SCHEMA varchar(64) DEFAULT NULL,
  OBJECT_TYPE varchar(64) DEFAULT NULL,
  OBJECT_SCHEMA varchar(64) DEFAULT NULL,
  OBJECT_NAME varchar(64) DEFAULT NULL,
  OBJECT_INSTANCE_BEGIN bigint(20) unsigned DEFAULT NULL,
  MYSQL_ERRNO int(11) DEFAULT NULL,
  RETURNED_SQLSTATE varchar(5) DEFAULT NULL,
  MESSAGE_TEXT varchar(128) DEFAULT NULL,
  ERRORS bigint(20) unsigned NOT NULL,
  WARNINGS bigint(20) unsigned NOT NULL,
  ROWS_AFFECTED bigint(20) unsigned NOT NULL,
  ROWS_SENT bigint(20) unsigned NOT NULL,
  ROWS_EXAMINED bigint(20) unsigned NOT NULL,
  CREATED_TMP_DISK_TABLES bigint(20) unsigned NOT NULL,
  CREATED_TMP_TABLES bigint(20) unsigned NOT NULL,
  SELECT_FULL_JOIN bigint(20) unsigned NOT NULL,
  SELECT_FULL_RANGE_JOIN bigint(20) unsigned NOT NULL,
  SELECT_RANGE bigint(20) unsigned NOT NULL,
  SELECT_RANGE_CHECK bigint(20) unsigned NOT NULL,
  SELECT_SCAN bigint(20) unsigned NOT NULL,
  SORT_MERGE_PASSES bigint(20) unsigned NOT NULL,
  SORT_RANGE bigint(20) unsigned NOT NULL,
  SORT_ROWS bigint(20) unsigned NOT NULL,
  SORT_SCAN bigint(20) unsigned NOT NULL,
  NO_INDEX_USED bigint(20) unsigned NOT NULL,
  NO_GOOD_INDEX_USED bigint(20) unsigned NOT NULL,
  NESTING_EVENT_ID bigint(20) unsigned DEFAULT NULL,
  NESTING_EVENT_TYPE enum('STATEMENT','STAGE','WAIT') DEFAULT NULL
); 
`

	CreateSomething2 = `
CREATE TABLE if not exists performance_schema.events_stages_history_long (
  THREAD_ID bigint(20) unsigned NOT NULL,
  EVENT_ID bigint(20) unsigned NOT NULL,
  END_EVENT_ID bigint(20) unsigned DEFAULT NULL,
  EVENT_NAME varchar(128) NOT NULL,
  SOURCE varchar(64) DEFAULT NULL,
  TIMER_START bigint(20) unsigned DEFAULT NULL,
  TIMER_END bigint(20) unsigned DEFAULT NULL,
  TIMER_WAIT bigint(20) unsigned DEFAULT NULL,
  NESTING_EVENT_ID bigint(20) unsigned DEFAULT NULL,
  NESTING_EVENT_TYPE enum('STATEMENT','STAGE','WAIT') DEFAULT NULL
);
`
	CreateSomething3 = `
CREATE TABLE if not exists performance_schema.events_waits_history_long (
  THREAD_ID bigint(20) unsigned NOT NULL,
  EVENT_ID bigint(20) unsigned NOT NULL,
  END_EVENT_ID bigint(20) unsigned DEFAULT NULL,
  EVENT_NAME varchar(128) NOT NULL,
  SOURCE varchar(64) DEFAULT NULL,
  TIMER_START bigint(20) unsigned DEFAULT NULL,
  TIMER_END bigint(20) unsigned DEFAULT NULL,
  TIMER_WAIT bigint(20) unsigned DEFAULT NULL,
  SPINS int(10) unsigned DEFAULT NULL,
  OBJECT_SCHEMA varchar(64) DEFAULT NULL,
  OBJECT_NAME varchar(512) DEFAULT NULL,
  INDEX_NAME varchar(64) DEFAULT NULL,
  OBJECT_TYPE varchar(64) DEFAULT NULL,
  OBJECT_INSTANCE_BEGIN bigint(20) unsigned NOT NULL,
  NESTING_EVENT_ID bigint(20) unsigned DEFAULT NULL,
  NESTING_EVENT_TYPE enum('STATEMENT','STAGE','WAIT') DEFAULT NULL,
  OPERATION varchar(32) NOT NULL,
  NUMBER_OF_BYTES bigint(20) DEFAULT NULL,
  FLAGS int(10) unsigned DEFAULT NULL
); 
`
	// CreateTiDBTable is the SQL statement creates a table in system db.
	// This table is a key-value struct contains some information used by TiDB.
	// Currently we only put bootstrapped in it which indicates if the system is already bootstrapped.
	CreateTiDBTable = `CREATE TABLE if not exists mysql.tidb(
		VARIABLE_NAME  VARCHAR(64) Not Null PRIMARY KEY,
		VARIABLE_VALUE VARCHAR(1024) DEFAULT Null,
		COMMENT VARCHAR(1024));`
)

// Bootstrap initiates system DB for a store.
func bootstrap(s Session) {
	b, err := checkBootstrapped(s)
	if err != nil {
		log.Fatal(err)
	}
	if b {
		return
	}
	doDDLWorks(s)
	doDMLWorks(s)
}

const (
	bootstrappedVar     = "bootstrapped"
	bootstrappedVarTrue = "True"
)

func checkBootstrapped(s Session) (bool, error) {
	//  Check if system db exists.
	_, err := s.Execute(fmt.Sprintf("USE %s;", mysql.SystemDB))
	if err != nil && infoschema.DatabaseNotExists.NotEqual(err) {
		log.Fatal(err)
	}
	// Check bootstrapped variable value in TiDB table.
	v, err := checkBootstrappedVar(s)
	if err != nil {
		return false, errors.Trace(err)
	}
	return v, nil
}

func checkBootstrappedVar(s Session) (bool, error) {
	sql := fmt.Sprintf(`SELECT VARIABLE_VALUE FROM %s.%s WHERE VARIABLE_NAME="%s"`,
		mysql.SystemDB, mysql.TiDBTable, bootstrappedVar)
	rs, err := s.Execute(sql)
	if err != nil {
		if infoschema.TableNotExists.Equal(err) {
			return false, nil
		}
		return false, errors.Trace(err)
	}

	if len(rs) != 1 {
		return false, errors.New("Wrong number of Recordset")
	}
	r := rs[0]
	row, err := r.Next()
	if err != nil || row == nil {
		return false, errors.Trace(err)
	}

	isBootstrapped := row.Data[0].(string) == bootstrappedVarTrue
	if isBootstrapped {
		// Make sure that doesn't affect the following operations.

		if err = s.FinishTxn(false); err != nil {
			return false, errors.Trace(err)
		}
	}

	return isBootstrapped, nil
}

// Execute DDL statements in bootstrap stage.
func doDDLWorks(s Session) {
	// Create a test database.
	mustExecute(s, "CREATE DATABASE IF NOT EXISTS test")
	// Create system db.
	mustExecute(s, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", mysql.SystemDB))
	// Create user table.
	mustExecute(s, CreateUserTable)
	// Create privilege tables.
	mustExecute(s, CreateDBPrivTable)
	mustExecute(s, CreateTablePrivTable)
	mustExecute(s, CreateColumnPrivTable)
	// Create global systemt variable table.
	mustExecute(s, CreateGloablVariablesTable)
	// Create proc table
	mustExecute(s, CreateProcTable)
	mustExecute(s, "CREATE DATABASE IF NOT EXISTS performance_schema;")
	mustExecute(s, CreateSomething1)
	mustExecute(s, CreateSomething2)
	mustExecute(s, CreateSomething3)
	// Create TiDB table.
	mustExecute(s, CreateTiDBTable)
}

// Execute DML statements in bootstrap stage.
// All the statements run in a single transaction.
func doDMLWorks(s Session) {
	mustExecute(s, "BEGIN")

	// Insert a default user with empty password.
	mustExecute(s, `INSERT INTO mysql.user VALUES
		("%", "root", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y")`)

	// Init global system variables table.
	values := make([]string, 0, len(variable.SysVars))
	for k, v := range variable.SysVars {
		value := fmt.Sprintf(`("%s", "%s")`, strings.ToLower(k), v.Value)
		values = append(values, value)
	}
	sql := fmt.Sprintf("INSERT INTO %s.%s VALUES %s;", mysql.SystemDB, mysql.GlobalVariablesTable,
		strings.Join(values, ", "))
	mustExecute(s, sql)

	sql = fmt.Sprintf(`INSERT INTO %s.%s VALUES("%s", "%s", "Bootstrap flag. Do not delete.")
		ON DUPLICATE KEY UPDATE VARIABLE_VALUE="%s"`,
		mysql.SystemDB, mysql.TiDBTable, bootstrappedVar, bootstrappedVarTrue, bootstrappedVarTrue)
	mustExecute(s, sql)
	mustExecute(s, "COMMIT")
}

func mustExecute(s Session, sql string) {
	_, err := s.Execute(sql)
	if err != nil {
		debug.PrintStack()
		log.Fatal(err)
	}
}
