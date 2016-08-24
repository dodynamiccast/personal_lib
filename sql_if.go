package main

import (
	"fmt"
	"reflect"
	_ "strings"
)

const (
	SQL_VAL   = 1
	SQL_LIMIT = 2

	SQL_VAL_NORMAL  = 101
	SQL_VAL_SQL_CAL = 102

	SQL_SELECT = 201
	SQL_INSERT = 202
	SQL_UPDATA = 203
	SQL_NORMAL = 204
	SQL_DELETE = 205
)

type SqlPara struct {
	colum      string
	value      interface{}
	op         string
	value_type int
}

var limitCol = []string{
	"id",
	"appid",
	"fileid",
}

type Limit struct {
	hasLimit int
}

func (p *Limit) CheckCol(sql_para SqlPara) {
	for _, para := range limitCol {
		if sql_para.colum == para && sql_para.op == "=" {
			p.hasLimit = 1
		}
	}
}

func JoinSql(para []interface{}, sql_type int) (string, []interface{}, int) {
	var in_para []interface{}
	var check Limit
	if sql_type == SQL_SELECT || sql_type == SQL_UPDATA || sql_type == SQL_NORMAL || sql_type == SQL_DELETE {
		for index, ivalue := range para {
			para_type := reflect.TypeOf(ivalue)
			kind := para_type.Kind()
			if kind == reflect.Struct {
				sql_para := ivalue.(SqlPara)
				check.CheckCol(sql_para)
				if sql_para.value_type != SQL_VAL_SQL_CAL {
					in_para = append(in_para, sql_para.value)
					sql_str := fmt.Sprint(sql_para.colum, sql_para.op, "?")
					para[index] = sql_str
				} else {
					sql_str := fmt.Sprint(sql_para.colum, sql_para.op, sql_para.value)
					para[index] = sql_str
				}
			}
		}
		if check.hasLimit != 1 {
			return "", nil, -1
		}
	}

	if sql_type == SQL_INSERT {
		for index, ivalue := range para {
			para_type := reflect.TypeOf(ivalue)
			kind := para_type.Kind()
			if kind == reflect.Struct {
				sql_para := ivalue.(SqlPara)
				in_para = append(in_para, sql_para.value)
				sql_str := fmt.Sprint(sql_para.colum, sql_para.op, "?")
				para[index] = sql_str
			}
			if kind == reflect.Slice || kind == reflect.Array {
				var col []interface{}
				var val []interface{}
				col = append(col, "(")
				val = append(val, "(")
				for i, iva_para := range ivalue.([]SqlPara) {
					col = append(col, iva_para.colum)
					if iva_para.value_type != SQL_VAL_SQL_CAL {
						val = append(val, "?")
						in_para = append(in_para, iva_para.value)
					} else {
						val = append(val, iva_para.value)
					}
					if i < len(ivalue.([]SqlPara))-1 {
						col = append(col, ",")
						val = append(val, ",")
					}
				}
				col = append(col, ")")
				val = append(val, ")")
				strCol := fmt.Sprint(col...)
				strVal := fmt.Sprint(val...)
				para[index] = fmt.Sprint(strCol, "values", strVal)
			}

		}
	}
	result := fmt.Sprintln(para...)
	return result, in_para, 0
}

func UpdateObj(table_name string, para [][2]interface{}, cond [][3]interface{}, ext string) (string, []interface{}, int) {
	var sql_para []interface{}
	strHead := fmt.Sprintf("update %s set", table_name)
	sql_para = append(sql_para, strHead)
	for i, elem := range para {
		elem_para := SqlPara{elem[0].(string), elem[1], "=", SQL_VAL_NORMAL}
		sql_para = append(sql_para, elem_para)
		if i < len(para)-1 {
			sql_para = append(sql_para, ",")
		}
	}
	if len(cond) > 0 {
		sql_para = append(sql_para, "where")
		for i, elem := range cond {
			elem_para := SqlPara{elem[0].(string), elem[1], elem[2].(string), SQL_VAL_NORMAL}
			sql_para = append(sql_para, elem_para)
			if i < len(cond)-1 {
				sql_para = append(sql_para, "and")
			}
		}
	}
	sql_para = append(sql_para, ext)
	return JoinSql(sql_para, SQL_UPDATA)
}

func InsertObj(table_name string, para [][2]interface{}) (string, []interface{}, int) {
	var sql_para []interface{}
	var insert_para []SqlPara
	strHead := fmt.Sprintf("insert into %s", table_name)
	sql_para = append(sql_para, strHead)
	for _, elem := range para {
		elem_para := SqlPara{elem[0].(string), elem[1], "=", SQL_VAL_NORMAL}
		insert_para = append(insert_para, elem_para)
	}
	sql_para = append(sql_para, insert_para)
	return JoinSql(sql_para, SQL_INSERT)
}

func SelectObj(table_name string, para [][2]interface{}, cond [][3]interface{}, extra string) (string, []interface{}, int) {
	var sql_para []interface{}
	sql_para = append(sql_para, "select")
	for i, elem := range para {
		//		elem_para := SqlPara{elem[0].(string), elem[1], "=", SQL_VAL_NORMAL}
		//		sql_para = append(sql_para, elem_para)
		if elem[1].(string) != "" {
			strPara := fmt.Sprintf("%s as %s", elem[0].(string), elem[1].(string))
			sql_para = append(sql_para, strPara)
		} else {
			sql_para = append(sql_para, elem[0])
		}
		if i < len(para)-1 {
			sql_para = append(sql_para, ",")
		}
	}
	sql_para = append(sql_para, "from")
	sql_para = append(sql_para, table_name)
	if len(cond) > 0 {
		sql_para = append(sql_para, "where")
		for i, elem := range cond {
			elem_para := SqlPara{elem[0].(string), elem[1], elem[2].(string), SQL_VAL_NORMAL}
			sql_para = append(sql_para, elem_para)
			if i < len(cond)-1 {
				sql_para = append(sql_para, "and")
			}
		}
	}
	return JoinSql(sql_para, SQL_UPDATA)
}

func DeleteObj(table_name string, cond [][3]interface{}) (string, []interface{}, int) {
	var sql_para []interface{}
	sql_para = append(sql_para, "delete from")
	sql_para = append(sql_para, table_name)
	if len(cond) > 0 {
		sql_para = append(sql_para, "where")
		for i, elem := range cond {
			elem_para := SqlPara{elem[0].(string), elem[1], elem[2].(string), SQL_VAL_NORMAL}
			sql_para = append(sql_para, elem_para)
			if i < len(cond)-1 {
				sql_para = append(sql_para, "and")
			}
		}
	}
	return JoinSql(sql_para, SQL_DELETE)
}
