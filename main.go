package main

import (
	"context"
	"database/sql"
	_ "database/sql"
	e "errors"
	"fmt"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/v1"
	"github.com/lib/pq"
	"io"
	"log"
	"math"
	"sort"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/lpichler/benchmark-sql-spicedb-poc/client"
)

var (
	spiceDBURL       = "localhost:50051"
	spiceDBToken     = "foobar"
	SpiceDbClient    *authzed.Client
	connectionString = "postgres://liborpichler:toor@localhost:5432/rbac?sslmode=disable"
)

func initSpiceDB() {
	if SpiceDbClient == nil {
		SpiceDbClient = client.InitServer(spiceDBURL, spiceDBToken)
	}
}

var benchmarkSpicedbUserLargeResults []time.Duration
var benchmarkSpicedbUserSmallResults []time.Duration
var benchmarkPureSpicedbUserMidResults []time.Duration
var benchmarkPureSpicedbUser60000Results []time.Duration

var benchmarkFDWUserLargeResults []time.Duration
var benchmarkFDWUserSmallResults []time.Duration
var benchmarkFDWUserMidResults []time.Duration
var benchmarkFDWUser60000Results []time.Duration

var benchmarkJoinQueryFWDUserLargeResults []time.Duration
var benchmarkJoinQueryFWDUserSmallResults []time.Duration
var benchmarkJoinQueryFWDUserMidResults []time.Duration
var benchmarkJoinQueryFWDUser60000Results []time.Duration

var benchmarkJoinQuerySQLSpicedbUserLargeResults []time.Duration
var benchmarkJoinQuerySQLSpicedbUserSmallResults []time.Duration
var benchmarkJoinQuerySQLSpicedbUserMidResults []time.Duration
var benchmarkJoinQuerySQLSpicedbUser60000Results []time.Duration

// Function 1 to measure
func benchmarkPureSpicedb1(userName, permission string) []string {
	ctx := context.Background()
	lrClient, err := SpiceDbClient.LookupResources(ctx, &v1.LookupResourcesRequest{
		ResourceObjectType: "workspace",
		Permission:         permission,
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: "user",
				ObjectId:   userName,
			},
		},
	})

	if err != nil {
		return []string{}
	}

	var workspaces []string
	for {
		next, err := lrClient.Recv()
		if e.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return []string{}
		}

		workspaces = append(workspaces, next.GetResourceObjectId())
	}

	return workspaces
}

// startTimer starts a timer for a block of code, and records the elapsed time.
func startTimer() time.Time {
	return time.Now()
}

// stopTimer stops the timer and records the elapsed time.
func stopTimer(startTime time.Time, results *[]time.Duration) {
	elapsedTime := time.Since(startTime)
	*results = append(*results, elapsedTime)
}

// displayStatistics calculates and displays statistics for timing results.
func displayStatistics(blockName string, results []time.Duration) {
	var total time.Duration
	minimum := time.Duration(math.MaxInt64)
	maximum := time.Duration(0)
	count := len(results)

	for _, duration := range results {
		total += duration
		if duration < minimum {
			minimum = duration
		}
		if duration > maximum {
			maximum = duration
		}
	}
	average := total / time.Duration(count)

	// Sort results for percentile calculation
	sortedResults := make([]time.Duration, len(results))
	copy(sortedResults, results)
	sort.Slice(sortedResults, func(i, j int) bool { return sortedResults[i] < sortedResults[j] })

	// Calculate percentiles
	p95 := sortedResults[int(float64(count)*0.95)-1]
	p99 := sortedResults[int(float64(count)*0.99)-1]

	// Display in table format
	fmt.Printf("Block: %s\n", blockName)
	fmt.Println("---------------------------------------------------------")
	fmt.Printf("| %12s | %12s | %12s | %12s | %12s |\n", "Min", "Max", "Average", "P95", "P99")
	fmt.Println("---------------------------------------------------------")
	fmt.Printf("| %12s | %12s | %12s | %12s | %12s |\n", minimum, maximum, average, p95, p99)
	fmt.Println("---------------------------------------------------------")
}

func openDB(connectionString string) *sql.DB {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatalf("Failed to open a DB connection: %v", err)
	}
	err = db.Ping()
	if err != nil {
		log.Fatalf("Failed to connect to the DB: %v", err)
	}
	return db
}

func queryDB(db *sql.DB, username, permission string) []string {
	query := `SELECT name FROM public.workspaces WHERE user_name=$1 AND permission=$2;`
	rows, err := db.Query(query, username, permission)
	if err != nil {
		log.Fatalf("Failed to execute a query: %v", err)
	}
	defer rows.Close()

	var workspaces []string

	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			log.Fatal(err)
		}
		//	fmt.Println(columnName)
		workspaces = append(workspaces, columnName)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	return workspaces
}

func queryDBJoin(db *sql.DB, workspaces []string) {
	if len(workspaces) > 65535 {
		return
	}

	placeholders := make([]string, len(workspaces))
	for i := range workspaces {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	query := fmt.Sprintf("SELECT COUNT(*) FROM items WHERE workspace IN (%s);", strings.Join(placeholders, ","))

	args := make([]interface{}, len(workspaces))
	for i, v := range workspaces {
		args[i] = v
	}

	row := db.QueryRow(query, args...)
	var count int
	if err := row.Scan(&count); err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func queryDBJoinUNEST(db *sql.DB, workspaces []string) {
	// Construct the query using UNNEST to handle the array of workspaces
	query := "SELECT COUNT(*) FROM items WHERE workspace IN (SELECT * FROM UNNEST($1::text[]));"

	// Convert workspaces slice to an interface{} using pq.Array for the query argument
	// This ensures the slice is passed correctly to the query

	// Execute the query
	row := db.QueryRow(query, pq.Array(workspaces))
	var count int
	if err := row.Scan(&count); err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("Count: %d\n", count)
	}
}

func queryDBJoinFWD(db *sql.DB, username, permission string) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM items INNER JOIN workspaces ON workspaces.name=items.workspace WHERE user_name=$1 AND permission=$2")

	rows, err := db.Query(query, username, permission)
	if err != nil {
		log.Fatalf("Failed to execute a query: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			log.Fatal(err)
		}
		//fmt.Println(columnName)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}

func main() {
	db := openDB(connectionString)
	defer db.Close()

	initSpiceDB()

	for i := 0; i < 100; i++ {
		startTime1 := startTimer()
		benchmarkPureSpicedb1("userlarge", "inventory_all_read")
		stopTimer(startTime1, &benchmarkSpicedbUserLargeResults)

		startTime2 := startTimer()
		benchmarkPureSpicedb1("usersmall", "inventory_all_read")
		stopTimer(startTime2, &benchmarkSpicedbUserSmallResults)

		startTime21 := startTimer()
		benchmarkPureSpicedb1("usermid", "inventory_all_read")
		stopTimer(startTime21, &benchmarkPureSpicedbUserMidResults)

		startTime22 := startTimer()
		benchmarkPureSpicedb1("user60000", "inventory_all_read")
		stopTimer(startTime22, &benchmarkPureSpicedbUser60000Results)

		startTime3 := startTimer()
		queryDB(db, "userlarge", "inventory_all_read")
		stopTimer(startTime3, &benchmarkFDWUserLargeResults)

		startTime4 := startTimer()
		queryDB(db, "usersmall", "inventory_all_read")
		stopTimer(startTime4, &benchmarkFDWUserSmallResults)

		startTime31 := startTimer()
		queryDB(db, "usermid", "inventory_all_read")
		stopTimer(startTime31, &benchmarkFDWUserMidResults)

		startTime32 := startTimer()
		queryDB(db, "user60000", "inventory_all_read")
		stopTimer(startTime32, &benchmarkFDWUser60000Results)

		startTime5 := startTimer()
		queryDBJoinFWD(db, "userlarge", "inventory_all_read")
		stopTimer(startTime5, &benchmarkJoinQueryFWDUserLargeResults)

		startTime6 := startTimer()
		queryDBJoinFWD(db, "usersmall", "inventory_all_read")
		stopTimer(startTime6, &benchmarkJoinQueryFWDUserSmallResults)

		startTime51 := startTimer()
		queryDBJoinFWD(db, "usermid", "inventory_all_read")
		stopTimer(startTime51, &benchmarkJoinQueryFWDUserMidResults)

		startTime52 := startTimer()
		queryDBJoinFWD(db, "user60000", "inventory_all_read")
		stopTimer(startTime52, &benchmarkJoinQueryFWDUser60000Results)

		startTime7 := startTimer()
		workspaces := benchmarkPureSpicedb1("userlarge", "inventory_all_read")
		queryDBJoinUNEST(db, workspaces)
		stopTimer(startTime7, &benchmarkJoinQuerySQLSpicedbUserLargeResults)

		startTime8 := startTimer()
		workspaces = benchmarkPureSpicedb1("usersmall", "inventory_all_read")
		queryDBJoin(db, workspaces)
		stopTimer(startTime8, &benchmarkJoinQuerySQLSpicedbUserSmallResults)

		startTime71 := startTimer()
		workspaces = benchmarkPureSpicedb1("usermid", "inventory_all_read")
		queryDBJoin(db, workspaces)
		stopTimer(startTime71, &benchmarkJoinQuerySQLSpicedbUserMidResults)

		startTime72 := startTimer()
		workspaces = benchmarkPureSpicedb1("user60000", "inventory_all_read")
		queryDBJoin(db, workspaces)
		stopTimer(startTime72, &benchmarkJoinQuerySQLSpicedbUser60000Results)
	}

	displayStatistics("SpiceDB call Only(large user)", benchmarkSpicedbUserLargeResults)
	displayStatistics("SpiceDB call Only(small user)", benchmarkSpicedbUserSmallResults)
	displayStatistics("SpiceDB call Only(usermid)", benchmarkPureSpicedbUserMidResults)
	displayStatistics("SpiceDB call Only(user60000)", benchmarkPureSpicedbUser60000Results)

	displayStatistics("FDW query(large user)", benchmarkFDWUserLargeResults)
	displayStatistics("FDW query(small user)", benchmarkFDWUserSmallResults)
	displayStatistics("FDW query(usermid)", benchmarkFDWUserMidResults)
	displayStatistics("FDW query(user60000)", benchmarkFDWUser60000Results)

	displayStatistics("Join SQL with FWD query(large user)", benchmarkJoinQueryFWDUserLargeResults)
	displayStatistics("Join SQL with FWD query(small user)", benchmarkJoinQueryFWDUserSmallResults)
	displayStatistics("Join SQL with FWD query(usermid)", benchmarkJoinQueryFWDUserMidResults)
	displayStatistics("Join SQL with FWD query(user60000)", benchmarkJoinQueryFWDUser60000Results)

	displayStatistics("Join SQL(UNNEST instead of IN) with SpiceDB call(large user)", benchmarkJoinQuerySQLSpicedbUserLargeResults)
	displayStatistics("Join SQL with SpiceDB call(small user)", benchmarkJoinQuerySQLSpicedbUserSmallResults)
	displayStatistics("Join SQL with SpiceDB call(usermid)", benchmarkJoinQuerySQLSpicedbUserMidResults)
	displayStatistics("Join SQL with SpiceDB call(usermid60000)", benchmarkJoinQuerySQLSpicedbUser60000Results)
}
