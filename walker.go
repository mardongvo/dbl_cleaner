package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

func hash_file_md5(filePath string) (string, error) {
	//Initialize variable returnMD5String now in case an error has to be returned
	var returnMD5String string

	//Open the passed argument and check for any error
	file, err := os.Open(filePath)
	if err != nil {
		return returnMD5String, err
	}

	//Tell the program to call the following function when the current function returns
	defer file.Close()

	//Open a new hash interface to write to
	hash := md5.New()

	//Copy the file in the hash interface and check for any error
	if _, err := io.Copy(hash, file); err != nil {
		return returnMD5String, err
	}

	//Get the 16 bytes hash
	hashInBytes := hash.Sum(nil)[:16]

	//Convert the bytes to a string
	returnMD5String = hex.EncodeToString(hashInBytes)

	return returnMD5String, nil

}

func checkExists(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}

func addDir(tx *sql.Tx, path string) int {
	var idd int
	for {
		rows, err := tx.Query("select ID from DIRS where PATH=?",
			path)
		reportFatal(err)
		defer rows.Close()
		for rows.Next() {
			err = rows.Scan(&idd)
			reportFatal(err)
		}
		reportFatal(rows.Err())
		if idd > 0 {
			return idd
		}
		_, err = tx.Exec("insert into DIRS(PATH) values (?)",
			path)
		reportFatal(err)
	}
	return 0
}

func addFile(tx *sql.Tx, path string, info os.FileInfo) {
	var filemod int64
	var filesize int64
	dir, fname := filepath.Split(path)
	parent := addDir(tx, dir)
	rows, err := tx.Query("select SIZE, LASTMOD from FILES where PARENT=? and FNAME=?",
		parent, fname)
	reportFatal(err)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&filesize, &filemod)
		reportFatal(err)
	}
	reportFatal(rows.Err())
	if (filesize == 0) && (filemod == 0) {
		hash, err := hash_file_md5(path)
		_, err = tx.Exec("insert into FILES(PARENT, FNAME, SIZE, LASTMOD, HASH) values (?,?,?,?,?)",
			parent, fname, info.Size(), info.ModTime().Unix(), hash)
		reportFatal(err)
	} else if (filesize != info.Size()) || (filemod != info.ModTime().Unix()) {
		hash, err := hash_file_md5(path)
		_, err = tx.Exec("update FILES set SIZE=?, LASTMOD=?, HASH=? where PARENT=? and FNAME=?",
			info.Size(), info.ModTime().Unix(), hash, parent, fname)
		reportFatal(err)
	}
}

func reportFatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	var save_counter int
	var tx *sql.Tx
	var dbpath string
	var root_path string
	var livecheck bool
	flag.StringVar(&dbpath, "db", "hash.sqlite", "path to db to save")
	flag.StringVar(&root_path, "root", "", "root folder to analyze")
	flag.BoolVar(&livecheck, "livecheck", false, "check files and folders existence")
	flag.Parse()
	db, err := sql.Open("sqlite3", dbpath)
	reportFatal(err)
	defer db.Close()

	sqlStmt := `
	CREATE TABLE IF NOT EXISTS DIRS (
	    ID      INTEGER PRIMARY KEY AUTOINCREMENT,
		PARENT  INTEGER,
	    PATH    TEXT,
		LIVE    INTEGER
	);
	CREATE TABLE IF NOT EXISTS FILES (
	    PARENT  INTEGER,
	    FNAME   TEXT,
	    SIZE    INTEGER,
	    LASTMOD TEXT,
	    HASH    TEXT,
		LIVE    INTEGER
	);
	CREATE INDEX IF NOT EXISTS I1 on DIRS(PATH);
	CREATE INDEX IF NOT EXISTS I2 on FILES(PARENT, FNAME);
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return
	}

	if root_path > "" {

		filepath.Walk(root_path, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			fmt.Println(path)
			if save_counter == 0 {
				tx, err = db.Begin()
				reportFatal(err)
			}
			save_counter++
			if info.IsDir() {
				addDir(tx, strings.ToUpper(path)+string(filepath.Separator))
			} else {
				addFile(tx, strings.ToUpper(path), info)
			}
			if save_counter >= 1000 {
				tx.Commit()
				save_counter = 0
			}
			return nil
		})
		tx.Commit()
	}

	{
		var spath string
		var idd int
		//folders parent
		tx, err = db.Begin()
		reportFatal(err)
		rows, err := tx.Query("select ID, PATH from DIRS where ID>1")
		reportFatal(err)
		defer rows.Close()
		for rows.Next() {
			err = rows.Scan(&idd, &spath)
			reportFatal(err)
			dir, _ := filepath.Split(spath[0 : len(spath)-1])
			if dir == spath {
				continue
			}
			parent := addDir(tx, dir)
			_, err = tx.Exec("update DIRS set PARENT=? where ID=?", parent, idd)
			reportFatal(err)
		}
		tx.Commit()
	}
	if livecheck {
		var idd int
		var path string
		var fname string
		//folders check
		tx, err = db.Begin()
		reportFatal(err)
		rows, err := tx.Query("select ID, PATH from DIRS")
		reportFatal(err)
		defer rows.Close()
		for rows.Next() {
			err = rows.Scan(&idd, &path)
			reportFatal(err)
			fmt.Println(path)
			if checkExists(path) {
				_, err = tx.Exec("update DIRS set LIVE=1 where ID=?", idd)
				reportFatal(err)
			} else {
				_, err = tx.Exec("update DIRS set LIVE=0 where ID=?", idd)
				reportFatal(err)
			}
		}
		reportFatal(rows.Err())
		tx.Commit()
		//files check
		tx, err = db.Begin()
		reportFatal(err)
		rows, err = tx.Query(`select FILES.PARENT, DIRS.PATH, FILES.FNAME from
		 FILES, DIRS where FILES.PARENT=DIRS.ID and DIRS.LIVE=1`)
		reportFatal(err)
		defer rows.Close()
		for rows.Next() {
			err = rows.Scan(&idd, &path, &fname)
			reportFatal(err)
			fmt.Println(filepath.Join(path, fname))
			if checkExists(filepath.Join(path, fname)) {
				_, err = tx.Exec("update FILES set LIVE=1 where PARENT=? and FNAME=?", idd, fname)
				reportFatal(err)
			} else {
				_, err = tx.Exec("update FILES set LIVE=0 where PARENT=? and FNAME=?", idd, fname)
				reportFatal(err)
			}
		}
		reportFatal(rows.Err())
		tx.Commit()
		tx, err = db.Begin()
		reportFatal(err)
		_, err = tx.Exec("delete from FILES where LIVE=0 or PARENT not in (select ID from DIRS where LIVE=1)")
		reportFatal(err)
		_, err = tx.Exec("delete from DIRS where LIVE=0")
		reportFatal(err)
		tx.Commit()
	}
}
