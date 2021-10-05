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
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

/*
(one)FileWalker generate FDEntry -> channel to Saver
-> (one)Saver: if FDEntry exists with same size and timestamp then skip
-> else send to (many)Hashers: calculate hash, send FDEntry to Saver
It can cause triple deadlock
*/

//file-directory entry
type FDEntry struct {
	path    string //dir path
	fname   string //dir - empty
	size    int64
	lastmod int64 //unix timestamp
	hash    string
	live    int
}

type HashDb struct {
	db           *sql.DB
	currTx       *sql.Tx
	save_counter int
}

//
type FileCounter struct {
	m            sync.Mutex
	countInput   int
	countHash    int
	walkerActive bool
}

func NewFileCounter() FileCounter {
	var fc FileCounter
	fc.walkerActive = true
	return fc
}

func (fc *FileCounter) walkerDone() {
	fc.m.Lock()
	defer fc.m.Unlock()
	fc.walkerActive = false
}

func (fc *FileCounter) incInput() {
	fc.m.Lock()
	defer fc.m.Unlock()
	fc.countInput++
}

func (fc *FileCounter) decInput() {
	fc.m.Lock()
	defer fc.m.Unlock()
	fc.countInput--
}

func (fc *FileCounter) inpToHash() {
	fc.m.Lock()
	defer fc.m.Unlock()
	fc.countInput--
	fc.countHash++
}

func (fc *FileCounter) hashToInp() {
	fc.m.Lock()
	defer fc.m.Unlock()
	fc.countInput++
	fc.countHash--
}

func (fc *FileCounter) isDone() bool {
	fc.m.Lock()
	defer fc.m.Unlock()
	return !fc.walkerActive && (fc.countInput == 0) && (fc.countHash == 0)
}

//

func hash_file_md5(filePath string) (string, error) {
	//Initialize variable returnMD5String now in case an error has to be returned
	var returnMD5String string
	//Open the passed argument and check for any error
	file, err := os.Open(filePath)
	if err != nil {
		return returnMD5String, err
	}
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

func (sv *HashDb) addDir(path string) int {
	var idd int
	for {
		rows, err := sv.currTx.Query("select ID from DIRS where PATH=?",
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
		_, err = sv.currTx.Exec("insert into DIRS(PATH) values (?)",
			path)
		reportFatal(err)
	}
	return 0
}

func (sv *HashDb) getInfo(fd FDEntry) (int64, int64) {
	var filemod int64
	var filesize int64
	parent := sv.addDir(fd.path)
	rows, err := sv.currTx.Query("select SIZE, LASTMOD from FILES where PARENT=? and FNAME=?",
		parent, fd.fname)
	reportFatal(err)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&filesize, &filemod)
		reportFatal(err)
	}
	reportFatal(rows.Err())
	return filesize, filemod
}

func (sv *HashDb) addFile(fd FDEntry) {
	var filemod int64
	var filesize int64
	parent := sv.addDir(fd.path)
	rows, err := sv.currTx.Query("select SIZE, LASTMOD from FILES where PARENT=? and FNAME=?",
		parent, fd.fname)
	reportFatal(err)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&filesize, &filemod)
		reportFatal(err)
	}
	reportFatal(rows.Err())
	if (filesize == 0) && (filemod == 0) {
		_, err = sv.currTx.Exec("insert into FILES(PARENT, FNAME, SIZE, LASTMOD, HASH) values (?,?,?,?,?)",
			parent, fd.fname, fd.size, fd.lastmod, fd.hash)
		reportFatal(err)
	} else if (filesize != fd.size) || (filemod != fd.lastmod) {
		_, err = sv.currTx.Exec("update FILES set SIZE=?, LASTMOD=?, HASH=? where PARENT=? and FNAME=?",
			fd.size, fd.lastmod, fd.hash, parent, fd.fname)
		reportFatal(err)
	}
}

func (sv *HashDb) Save() {
	var err error
	sv.save_counter++
	if sv.save_counter >= 1000 {
		sv.currTx.Commit()
		sv.save_counter = 0
	}
	if sv.save_counter == 0 {
		sv.currTx, err = sv.db.Begin()
		reportFatal(err)
	}
}

func (sv *HashDb) Commit() {
	var err error
	sv.save_counter = 0
	sv.currTx.Commit()
	sv.currTx, err = sv.db.Begin()
	reportFatal(err)
}

func (sv *HashDb) Init(dbpath string) {
	var err error
	sv.db, err = sql.Open("sqlite3", dbpath)
	reportFatal(err)
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
	_, err = sv.db.Exec(sqlStmt)
	reportFatal(err)
	sv.currTx, err = sv.db.Begin()
	reportFatal(err)
}

func (sv *HashDb) Close() {
	if (sv.db != nil) && (sv.currTx != nil) {
		sv.currTx.Commit()
	}
	if sv.db != nil {
		sv.db.Close()
	}
}

func (sv *HashDb) addParents() {
	var spath string
	var idd int
	var parent int
	var doit = true
	for doit {
		doit = false
		rows, err := sv.currTx.Query("select ID, PATH from DIRS where ID>0 and PARENT is null")
		reportFatal(err)
		for rows.Next() {
			doit = true
			err = rows.Scan(&idd, &spath)
			reportFatal(err)
			fmt.Println(spath)
			if len(spath) == 0 {
				continue
			}
			dir, _ := filepath.Split(spath[0 : len(spath)-1])
			if dir == spath {
				continue
			}
			if dir == "" {
				parent = 0
			} else {
				parent = sv.addDir(dir)
			}
			_, err = sv.currTx.Exec("update DIRS set PARENT=? where ID=?", parent, idd)
			reportFatal(err)
		}
		rows.Close()
		sv.Save()
	}
}

func (sv *HashDb) liveCheck() {
	var idd int
	var path string
	var fname string
	var set1, set0 []int
	//folders check
	sv.Commit()
	rows, err := sv.currTx.Query("select ID, PATH from DIRS")
	reportFatal(err)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&idd, &path)
		reportFatal(err)
		fmt.Println(path)
		if checkExists(path) {
			set1 = append(set1, idd)
		} else {
			set0 = append(set0, idd)
		}
	}
	reportFatal(rows.Err())
	for _, idd = range set0 {
		_, err = sv.currTx.Exec("update DIRS set LIVE=0 where ID=?", idd)
		reportFatal(err)
	}
	for _, idd = range set1 {
		_, err = sv.currTx.Exec("update DIRS set LIVE=1 where ID=?", idd)
		reportFatal(err)
	}
	sv.Commit()
	//files check
	set0 = nil
	set1 = nil
	rows, err = sv.currTx.Query(`select FILES.PARENT, DIRS.PATH, FILES.FNAME from
		 FILES, DIRS where FILES.PARENT=DIRS.ID and DIRS.LIVE=1`)
	reportFatal(err)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&idd, &path, &fname)
		reportFatal(err)
		fmt.Println(filepath.Join(path, fname))
		if checkExists(filepath.Join(path, fname)) {
			_, err = sv.currTx.Exec("update FILES set LIVE=1 where PARENT=? and FNAME=?", idd, fname)
			reportFatal(err)
		} else {
			_, err = sv.currTx.Exec("update FILES set LIVE=0 where PARENT=? and FNAME=?", idd, fname)
			reportFatal(err)
		}
	}
	reportFatal(rows.Err())
	sv.Commit()
	_, err = sv.currTx.Exec("delete from FILES where LIVE=0 or PARENT not in (select ID from DIRS where LIVE=1)")
	reportFatal(err)
	_, err = sv.currTx.Exec("delete from DIRS where LIVE=0")
	reportFatal(err)
	sv.Commit()
}

func reportFatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// 1.
func FileWalker(rootPath string, fdOut chan<- FDEntry, cntr *FileCounter,
	wg *sync.WaitGroup) {
	filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		var fd FDEntry
		if err != nil {
			log.Println(err)
			return nil
		}
		if info.IsDir() {
			fd.path = path
			fd.lastmod = info.ModTime().Unix()
			cntr.incInput()
			fdOut <- fd
			return nil
		}
		//fmt.Println(path)
		fd.path, fd.fname = filepath.Split(path)
		fd.size = info.Size()
		fd.lastmod = info.ModTime().Unix()
		cntr.incInput()
		fmt.Println("FW", filepath.Join(fd.path, fd.fname))
		fdOut <- fd
		return nil
	})
	cntr.walkerDone()
	wg.Done()
}

// 2.
func Hasher(fdInp <-chan FDEntry, fdOut chan<- FDEntry, cntr *FileCounter,
	wg *sync.WaitGroup) {
	var err error
	for fd := range fdInp {
		fmt.Println("HH", filepath.Join(fd.path, fd.fname))
		fd.hash, err = hash_file_md5(filepath.Join(fd.path, fd.fname))
		if err != nil {
			fmt.Println(filepath.Join(fd.path, fd.fname), err)
			fd.hash = "-"
		}
		cntr.hashToInp()
		fdOut <- fd
	}
	wg.Done()
}

// 3.
func Saver(hdb *HashDb, fdInp <-chan FDEntry, fdOut chan<- FDEntry,
	cntr *FileCounter, wg *sync.WaitGroup) {
	for fd := range fdInp {
		fmt.Println(filepath.Join(fd.path, fd.fname))
		if fd.fname == "" {
			hdb.addDir(fd.path)
			hdb.Save()
			cntr.decInput()
		} else {
			if fd.size == 0 {
				hdb.addFile(fd)
				hdb.Save()
				cntr.decInput()
				continue
			}
			if fd.hash == "" {
				fsize, fmod := hdb.getInfo(fd)
				if (fd.size == fsize) && (fd.lastmod == fmod) {
					cntr.decInput()
				} else {
					cntr.inpToHash()
					fdOut <- fd
				}
			} else {
				hdb.addFile(fd)
				hdb.Save()
				cntr.decInput()
			}
		}
	}
	hdb.Commit()
	wg.Done()
}

func main() {
	var dbpath string
	var root_path string
	var livecheck bool
	var hash_count int
	var sv HashDb
	var wg1 sync.WaitGroup
	flag.StringVar(&dbpath, "db", "hash.sqlite", "path to db to save")
	flag.StringVar(&root_path, "root", "", "root folder to analyze")
	flag.BoolVar(&livecheck, "livecheck", false, "check files and folders existence")
	flag.IntVar(&hash_count, "n", 1, "number of hashers")
	flag.Parse()
	sv.Init(dbpath)
	defer sv.Close()

	if root_path > "" {
		counter := NewFileCounter()
		walkerChan := make(chan FDEntry, hash_count)
		hasherChan := make(chan FDEntry, hash_count)
		wg1.Add(1)
		go FileWalker(root_path, walkerChan, &counter, &wg1)
		wg1.Add(1)
		go Saver(&sv, walkerChan, hasherChan, &counter, &wg1)
		for i := 0; i < hash_count; i++ {
			wg1.Add(1)
			go Hasher(hasherChan, walkerChan, &counter, &wg1)
		}
		for {
			fmt.Printf("%d-%d\n", counter.countInput, counter.countHash)
			if counter.isDone() {
				close(walkerChan)
				close(hasherChan)
				break
			}
			time.Sleep(1 * time.Second)
		}
		wg1.Wait()
		sv.Commit()
		sv.addParents()
		sv.Commit()
	}

	if livecheck {
		sv.liveCheck()
	}
}
