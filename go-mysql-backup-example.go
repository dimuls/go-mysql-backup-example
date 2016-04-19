package main

import (
	"archive/tar"
	"compress/gzip"
	"database/sql"
	"encoding/csv"
	"flag"
	"io"
	"log"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	_ "github.com/go-sql-driver/mysql"
)

type configuration struct {
	Databases  []string
	BackupPath string
}

type user struct {
	DBID int
	ID   int
	Name string
}

type order struct {
	DBID        int
	ID          int
	UserID      int
	OrderAmount float64
}

type csvLine []string

func readConfiguration(path string) configuration {
	_, err := os.Stat(path)
	if err != nil {
		log.Fatalln("Config file is missing:", path)
	}
	var config configuration
	if _, err := toml.DecodeFile(path, &config); err != nil {
		log.Fatalln(err)
	}
	return config
}

func checkErr(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

func usersFetcher(dsns []string) <-chan user {
	fetchers := sync.WaitGroup{}
	users := make(chan user)
	for id, dsn := range dsns {
		handle, err := sql.Open("mysql", dsn)
		checkErr(err)
		rows, err := handle.Query("SELECT user_id, name FROM users")
		checkErr(err)
		fetchers.Add(1)
		go func(DBID int) {
			for rows.Next() {
				u := user{DBID: DBID}
				err = rows.Scan(&u.ID, &u.Name)
				checkErr(err)
				users <- u
			}
			handle.Close()
			fetchers.Done()
		}(id)
	}
	go func() {
		fetchers.Wait()
		close(users)
	}()
	return users
}

func ordersFetcher(dsns []string) <-chan order {
	fetchers := sync.WaitGroup{}
	orders := make(chan order)
	for id, dsn := range dsns {
		handle, err := sql.Open("mysql", dsn)
		checkErr(err)
		rows, err := handle.Query("SELECT order_id, user_id, order_amount FROM sales")
		checkErr(err)
		fetchers.Add(1)
		go func(DBID int) {
			for rows.Next() {
				o := order{DBID: DBID}
				err = rows.Scan(&o.ID, &o.UserID, &o.OrderAmount)
				checkErr(err)
				orders <- o
			}
			handle.Close()
			fetchers.Done()
		}(id)
	}
	go func() {
		fetchers.Wait()
		close(orders)
	}()
	return orders
}

func addFileToArchive(tw *tar.Writer, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	if stat, err := file.Stat(); err == nil {
		// now lets create the header as needed for this file within the tarball
		header := new(tar.Header)
		header.Name = path
		header.Size = stat.Size()
		header.Mode = int64(stat.Mode())
		header.ModTime = stat.ModTime()
		// write the header to the tarball archive
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		// copy the file data to the tarball
		if _, err := io.Copy(tw, file); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	log.Println("Backup started...")

	var configPath string
	var backupPath string

	flag.StringVar(&configPath,
		"config",
		"./go-mysql-backup-example.conf",
		"go-mysql-backup-example config file",
	)

	flag.StringVar(&backupPath,
		"backup",
		"./",
		"go-mysql-backup-example backup path",
	)

	flag.Parse()

	config := readConfiguration(configPath)
	if len(config.BackupPath) == 0 {
		config.BackupPath = backupPath
	}

	err := os.MkdirAll(config.BackupPath, 0777)
	checkErr(err)

	backupers := sync.WaitGroup{}

	usersCount := 0
	usersCsvPath := path.Join(config.BackupPath, "users.csv")
	usersFile, err := os.OpenFile(usersCsvPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	checkErr(err)
	usersCsv := csv.NewWriter(usersFile)
	users := usersFetcher(config.Databases)
	backupers.Add(1)
	go func() {
		for u := range users {
			usersCount += 1
			usersCsv.Write(csvLine{
				strconv.Itoa(u.DBID),
				strconv.Itoa(u.ID),
				u.Name,
			})
		}
		usersCsv.Flush()
		usersFile.Close()
		backupers.Done()
	}()

	ordersCount := 0
	ordersCsvPath := path.Join(config.BackupPath, "sales.csv")
	ordersFile, err := os.OpenFile(ordersCsvPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	checkErr(err)
	ordersCsv := csv.NewWriter(ordersFile)
	orders := ordersFetcher(config.Databases)
	backupers.Add(1)
	go func() {
		for o := range orders {
			ordersCount++
			ordersCsv.Write(csvLine{
				strconv.Itoa(o.DBID),
				strconv.Itoa(o.ID),
				strconv.Itoa(o.UserID),
				strconv.FormatFloat(o.OrderAmount, 'f', 6, 64),
			})
		}
		ordersCsv.Flush()
		ordersFile.Close()
		backupers.Done()
	}()

	backupers.Wait()
	log.Println("Backup done.")
	log.Printf("Backuped %d users and %d orders.", usersCount, ordersCount)
	log.Println("Archive started...")

	archivesPath := path.Join(config.BackupPath, "archive")
	os.MkdirAll(archivesPath, 0777)

	archivePath := path.Join(archivesPath, time.Now().Format("backup-2006-01-02-15-04-05.tar.gz"))
	archive, err := os.Create(archivePath)
	checkErr(err)
	defer archive.Close()
	archiveGz := gzip.NewWriter(archive)
	defer archiveGz.Close()
	archiveTar := tar.NewWriter(archiveGz)
	defer archiveTar.Close()
	checkErr(addFileToArchive(archiveTar, usersCsvPath))
	checkErr(addFileToArchive(archiveTar, ordersCsvPath))

	log.Println("Archive done")
}
