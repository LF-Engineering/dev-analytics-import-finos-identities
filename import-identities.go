package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	"gopkg.in/yaml.v2"
)

const cOrigin = "bitergia-import-sh-json"

// gProjectSlug comes from PROJECT_SLUG env (if set)
var gProjectSlug *string

// shTime - used to parse non standart time format in Bitergia JSON
type shTime struct {
	time.Time
	Set bool
}

// shCountry - country data
type shCountry struct {
	Alpha3 string `json:"alpha3"`
	Code   string `json:"code"`
	Name   string `json:"name"`
}

// shProfile - singleprofile data
type shProfile struct {
	Country     *shCountry `json:"country"`
	Email       *string    `json:"email"`
	Gender      *string    `json:"gender"`
	GenderAcc   *int       `json:"gender_acc"`
	IsBot       *bool      `json:"is_bot"`
	Name        *string    `json:"name"`
	UUID        string     `json:"uuid"`
	CountryCode *string
}

// shIdentity - signgle identity data
type shIdentity struct {
	Email        *string `json:"email"`
	ID           string  `json:"id"`
	Name         *string `json:"name"`
	Source       string  `json:"source"`
	Username     *string `json:"username"`
	UUID         string  `json:"uuid"`
	LastModified time.Time
}

// shEnrollment - single company enrollment data
type shEnrollment struct {
	UUID         string `json:"uuid"`
	Organization string `json:"organization"`
	Start        shTime `json:"start"`
	End          shTime `json:"end"`
	OrgID        int
	ProjectSlug  *string
}

// shUIdentity - single unique identity data
type shUIdentity struct {
	UUID         string         `json:"uuid"`
	Profile      shProfile      `json:"profile"`
	Identities   []shIdentity   `json:"identities"`
	Enrollments  []shEnrollment `json:"enrollments"`
	LastModified time.Time
}

// shData - Bitergia's identities export data format
type shData struct {
	UIdentities map[string]shUIdentity `json:"uidentities"`
}

// importStats - statistics about added/updated/deleted objects
type importStats struct {
	uidentitiesAdded   int
	uidentitiesFound   int
	profilesAdded      int
	profilesFound      int
	profilesSame       int
	profilesDeleted    int
	identitiesAdded    int
	identitiesFound    int
	identitiesSame     int
	identitiesDeleted  int
	enrollmentsAdded   int
	enrollmentsFound   int
	enrollmentsSame    int
	enrollmentsDeleted int
	enrollmentsSkipped int
}

// allmappings - company names mapping from dev-analytics-affiliation
type allMappings struct {
	Mappings [][2]string `yaml:"mappings"`
}

const nils string = "(nil)"
const emailStr string = ",Email:"

func (p *shProfile) String() (s string) {
	s = "{UUID:" + p.UUID + ",Name:"
	if p.Name != nil {
		s += *p.Name
	} else {
		s += nils
	}
	s += emailStr
	if p.Email != nil {
		s += *p.Email
	} else {
		s += nils
	}
	s += ",Gender:"
	if p.Gender != nil {
		s += *p.Gender
	} else {
		s += nils
	}
	s += ",GenderAcc:"
	if p.GenderAcc != nil {
		s += fmt.Sprintf("%d", *p.GenderAcc)
	} else {
		s += nils
	}
	s += ",IsBot:"
	if p.IsBot != nil {
		s += fmt.Sprintf("%v", *p.IsBot)
	} else {
		s += nils
	}
	s += ",CountryCode:"
	if p.CountryCode != nil {
		s += *p.CountryCode
	} else {
		s += nils
	}
	s += "}"
	return
}

func (i *shIdentity) String() (s string) {
	s = "{UUID:" + i.UUID + ",ID:" + i.ID + ",Source:" + i.Source + ",Name:"
	if i.Name != nil {
		s += *i.Name
	} else {
		s += nils
	}
	s += emailStr
	if i.Email != nil {
		s += *i.Email
	} else {
		s += nils
	}
	s += ",Username:"
	if i.Username != nil {
		s += *i.Username
	} else {
		s += nils
	}
	s += "}"
	return
}

func (sht *shTime) String() string {
	return sht.Format("2006-01-02")
}

func (e *shEnrollment) String() (s string) {
	s = fmt.Sprintf("{UUID:%s,Organization:%s,OrgID:%d,From:%s,End:%s,ProjectSlug:", e.UUID, e.Organization, e.OrgID, e.Start.String(), e.End.String())
	if e.ProjectSlug != nil {
		s += *e.ProjectSlug + "}"
	} else {
		s += nils + "}"
	}
	return
}

func fatalOnError(err error) {
	if err != nil {
		tm := time.Now()
		fmt.Printf("Error(time=%+v):\nError: '%s'\nStacktrace:\n%s\n", tm, err.Error(), string(debug.Stack()))
		fmt.Fprintf(os.Stderr, "Error(time=%+v):\nError: '%s'\nStacktrace:\n", tm, err.Error())
		panic("stacktrace")
	}
}

func fatalf(f string, a ...interface{}) {
	fatalOnError(fmt.Errorf(f, a...))
}

func (sht *shTime) UnmarshalJSON(b []byte) (err error) {
	s := strings.Trim(string(b), "\"")
	if s == "null" {
		return
	}
	dtFmt := "2006-01-02T15:04:05"
	sht.Time, err = time.Parse(dtFmt, s)
	if err == nil {
		sht.Set = true
	}
	return
}

func queryOut(query string, args ...interface{}) {
	fmt.Printf("%s\n", query)
	if len(args) > 0 {
		s := ""
		for vi, vv := range args {
			switch v := vv.(type) {
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, complex64, complex128, string, bool, time.Time:
				s += fmt.Sprintf("%d:%+v ", vi+1, v)
			case *int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64, *float32, *float64, *complex64, *complex128, *string, *bool, *time.Time:
				s += fmt.Sprintf("%d:%+v ", vi+1, v)
			case nil:
				s += fmt.Sprintf("%d:(null) ", vi+1)
			default:
				s += fmt.Sprintf("%d:%+v ", vi+1, reflect.ValueOf(vv).Elem())
			}
		}
		fmt.Printf("[%s]\n", s)
	}
}

func query(db *sql.DB, query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		queryOut(query, args...)
	}
	return rows, err
}

func exec(db *sql.DB, skip, query string, args ...interface{}) (sql.Result, error) {
	res, err := db.Exec(query, args...)
	if err != nil {
		if skip == "" || !strings.Contains(err.Error(), skip) {
			queryOut(query, args...)
		}
	}
	return res, err
}

func addOrganization(db *sql.DB, company string) (int, bool) {
	_, err := exec(db, "Error 1062", "insert into organizations(name) values(?)", stripUnicodeStr(company))
	exists := false
	if err != nil {
		if strings.Contains(err.Error(), "Error 1062") {
			rows, err2 := query(db, "select name from organizations where name = ?", company)
			fatalOnError(err2)
			var existingName string
			for rows.Next() {
				fatalOnError(rows.Scan(&existingName))
			}
			fatalOnError(rows.Err())
			fatalOnError(rows.Close())
			exists = true
		} else {
			fatalOnError(err)
		}
	}
	rows, err := query(db, "select id from organizations where name = ?", company)
	fatalOnError(err)
	var id int
	fetched := false
	for rows.Next() {
		fatalOnError(rows.Scan(&id))
		fetched = true
	}
	fatalOnError(rows.Err())
	fatalOnError(rows.Close())
	if !fetched {
		fatalf("failed to add '%s' company", company)
	}
	return id, exists
}

func addCountry(db *sql.DB, country *shCountry) (exists bool) {
	_, err := exec(
		db,
		"Error 1062",
		"insert into countries(code, alpha3, name) values(?,?,?)",
		country.Code,
		country.Alpha3,
		stripUnicodeStr(country.Name),
	)
	if err != nil {
		if strings.Contains(err.Error(), "Error 1062") {
			exists = true
		} else {
			fatalOnError(err)
		}
	}
	return
}

func getThreadsNum() int {
	// Use environment variable to have singlethreaded version
	st := os.Getenv("ST") != ""
	if st {
		return 1
	}
	nCPUs := 0
	if os.Getenv("NCPUS") != "" {
		n, err := strconv.Atoi(os.Getenv("NCPUS"))
		fatalOnError(err)
		if n > 0 {
			nCPUs = n
		}
	}
	if nCPUs > 0 {
		n := runtime.NumCPU()
		if nCPUs > n {
			nCPUs = n
		}
		runtime.GOMAXPROCS(nCPUs)
		return nCPUs
	}
	nCPUs = runtime.NumCPU()
	runtime.GOMAXPROCS(nCPUs)
	return nCPUs
}

func stripUnicode(pStr *string) *string {
	if pStr == nil {
		return nil
	}
	str := *pStr
	isOk := func(r rune) bool {
		return r < 32 || r >= 127
	}
	t := transform.Chain(norm.NFKD, transform.RemoveFunc(isOk))
	str, _, _ = transform.String(t, str)
	return &str
}

func stripUnicodeStr(str string) string {
	isOk := func(r rune) bool {
		return r < 32 || r >= 127
	}
	t := transform.Chain(norm.NFKD, transform.RemoveFunc(isOk))
	str, _, _ = transform.String(t, str)
	return str
}

func cleanUTF8(str string) string {
	if strings.Contains(str, "\x00") {
		return strings.Replace(str, "\x00", "", -1)
	}
	return str
}

func truncToBytes(str string, size int) string {
	str = cleanUTF8(str)
	length := len(str)
	if length < size {
		return str
	}
	res := ""
	i := 0
	for _, r := range str {
		if len(res+string(r)) > size {
			break
		}
		res += string(r)
		i++
	}
	return res
}

func truncStringOrNil(strPtr *string, maxLen int) interface{} {
	if strPtr == nil {
		return nil
	}
	return truncToBytes(*strPtr, maxLen)
}

func profilesDiffer(p1, p2 *shProfile) bool {
	if p1.Name == nil && p2.Name != nil || p1.Name != nil && p2.Name == nil {
		return true
	}
	if p1.Name != nil && p2.Name != nil && stripUnicodeStr(*p1.Name) != stripUnicodeStr(*p2.Name) {
		return true
	}
	if p1.Email == nil && p2.Email != nil || p1.Email != nil && p2.Email == nil {
		return true
	}
	if p1.Email != nil && p2.Email != nil && stripUnicodeStr(*p1.Email) != stripUnicodeStr(*p2.Email) {
		return true
	}
	if p1.GenderAcc == nil && p2.GenderAcc != nil || p1.GenderAcc != nil && p2.GenderAcc == nil {
		return true
	}
	if p1.GenderAcc != nil && p2.GenderAcc != nil && *p1.GenderAcc != *p2.GenderAcc {
		return true
	}
	if p1.IsBot == nil && p2.IsBot != nil || p1.IsBot != nil && p2.IsBot == nil {
		return true
	}
	if p1.IsBot != nil && p2.IsBot != nil && *p1.IsBot != *p2.IsBot {
		return true
	}
	if p1.CountryCode == nil && p2.CountryCode != nil || p1.CountryCode != nil && p2.CountryCode == nil {
		return true
	}
	if p1.CountryCode != nil && p2.CountryCode != nil && truncToBytes(*p1.CountryCode, 2) != truncToBytes(*p2.CountryCode, 2) {
		return true
	}
	return false
}

func identitiesDiffer(i1, i2 *shIdentity) bool {
	if i1.UUID != i2.UUID {
		return true
	}
	if i1.ID != i2.ID {
		return true
	}
	if i1.Source != i2.Source {
		return true
	}
	if i1.Name == nil && i2.Name != nil || i1.Name != nil && i2.Name == nil {
		return true
	}
	if i1.Name != nil && i2.Name != nil && stripUnicodeStr(*i1.Name) != stripUnicodeStr(*i2.Name) {
		return true
	}
	if i1.Email == nil && i2.Email != nil || i1.Email != nil && i2.Email == nil {
		return true
	}
	if i1.Email != nil && i2.Email != nil && stripUnicodeStr(*i1.Email) != stripUnicodeStr(*i2.Email) {
		return true
	}
	if i1.Username == nil && i2.Username != nil || i1.Username != nil && i2.Username == nil {
		return true
	}
	if i1.Username != nil && i2.Username != nil && stripUnicodeStr(*i1.Username) != stripUnicodeStr(*i2.Username) {
		return true
	}
	return false
}

func enrollmentsDiffer(e1, e2 []shEnrollment) bool {
	m1 := make(map[string]struct{})
	m2 := make(map[string]struct{})
	for _, enrollment := range e1 {
		m1[enrollment.String()] = struct{}{}
	}
	for _, enrollment := range e2 {
		m2[enrollment.String()] = struct{}{}
	}
	for k1 := range m1 {
		_, ok := m2[k1]
		if !ok {
			return true
		}
	}
	for k2 := range m2 {
		_, ok := m1[k2]
		if !ok {
			return true
		}
	}
	return false
}

func processUIdentity(ch chan struct{}, mtx *sync.RWMutex, db *sql.DB, uidentity shUIdentity, comp2id map[string]int, id2comp map[int]string, flags []bool, stats *importStats) {
	defer func() {
		if ch != nil {
			ch <- struct{}{}
		}
	}()
	_, _ = db.Exec("set @origin = ?", cOrigin)
	var sts importStats
	dbg := flags[0]
	replace := flags[1]
	compare := flags[2]
	orgsRO := flags[3]
	rows, err := query(db, "select uuid from uidentities where uuid = ?", uidentity.UUID)
	fatalOnError(err)
	uuid := uidentity.UUID
	fetched := false
	for rows.Next() {
		fatalOnError(rows.Scan(&uuid))
		fetched = true
		break
	}
	fatalOnError(rows.Err())
	fatalOnError(rows.Close())
	if !fetched {
		_, err := exec(
			db,
			"",
			"insert into uidentities(uuid, last_modified) values(?,now())",
			uidentity.UUID,
		)
		fatalOnError(err)
		sts.uidentitiesAdded++
	} else {
		sts.uidentitiesFound++
	}
	var existingProfile shProfile
	rows, err = query(
		db,
		"select uuid, name, email, gender, gender_acc, is_bot, country_code from profiles where uuid = ?",
		uidentity.UUID,
	)
	fatalOnError(err)
	fetched = false
	for rows.Next() {
		fatalOnError(
			rows.Scan(
				&existingProfile.UUID,
				&existingProfile.Name,
				&existingProfile.Email,
				&existingProfile.Gender,
				&existingProfile.GenderAcc,
				&existingProfile.IsBot,
				&existingProfile.CountryCode,
			),
		)
		fetched = true
		break
	}
	fatalOnError(rows.Err())
	fatalOnError(rows.Close())
	if fetched {
		sts.profilesFound++
	}
	same := false
	if fetched && compare {
		if uidentity.Profile.Country != nil {
			uidentity.Profile.CountryCode = &uidentity.Profile.Country.Code
		}
		same = !profilesDiffer(&uidentity.Profile, &existingProfile)
		if same {
			sts.profilesSame++
		} else if dbg {
			fmt.Printf("Profiles differ: %+v != %+v\n", uidentity.Profile, existingProfile)
		}
	}
	if fetched && !same && replace {
		_, err := exec(db, "", "delete from profiles where uuid = ?", uidentity.UUID)
		fatalOnError(err)
		sts.profilesDeleted++
	}
	if !same && (!fetched || (fetched && replace)) {
		if uidentity.Profile.Country != nil {
			uidentity.Profile.CountryCode = &uidentity.Profile.Country.Code
		}
		_, err := exec(
			db,
			"",
			"insert into profiles(uuid, name, email, gender, gender_acc, is_bot, country_code) values(?,?,?,?,?,?,?)",
			uidentity.UUID,
			stripUnicode(uidentity.Profile.Name),
			stripUnicode(uidentity.Profile.Email),
			uidentity.Profile.Gender,
			uidentity.Profile.GenderAcc,
			uidentity.Profile.IsBot,
			truncStringOrNil(uidentity.Profile.CountryCode, 2),
		)
		fatalOnError(err)
		sts.profilesAdded++
	}
	for _, identity := range uidentity.Identities {
		var existingIdentity shIdentity
		rows, err = query(
			db,
			"select uuid, id, email, name, source, username from identities where id = ? or (name = ? and email = ? and username = ? and source = ?)",
			identity.ID,
			stripUnicode(identity.Name),
			stripUnicode(identity.Email),
			stripUnicode(identity.Username),
			identity.Source,
		)
		fatalOnError(err)
		fetched = false
		for rows.Next() {
			fatalOnError(
				rows.Scan(
					&existingIdentity.UUID,
					&existingIdentity.ID,
					&existingIdentity.Email,
					&existingIdentity.Name,
					&existingIdentity.Source,
					&existingIdentity.Username,
				),
			)
			fetched = true
			break
		}
		fatalOnError(rows.Err())
		fatalOnError(rows.Close())
		if fetched {
			sts.identitiesFound++
		}
		same = false
		if fetched && compare {
			same = !identitiesDiffer(&identity, &existingIdentity)
			if same {
				sts.identitiesSame++
			} else if dbg {
				fmt.Printf("Identities differ: %+v != %+v\n", identity, existingIdentity)
			}
		}
		if fetched && !same && replace {
			_, err := exec(
				db,
				"",
				"delete from identities where id = ? or (name = ? and email = ? and username = ? and source = ?)",
				identity.ID,
				stripUnicode(identity.Name),
				stripUnicode(identity.Email),
				stripUnicode(identity.Username),
				identity.Source,
			)
			fatalOnError(err)
			sts.identitiesDeleted++
		}
		if !same && (!fetched || (fetched && replace)) {
			_, err := exec(
				db,
				"",
				"insert into identities(uuid, id, source, name, email, username, last_modified) values(?,?,?,?,?,?,now())",
				identity.UUID,
				identity.ID,
				identity.Source,
				stripUnicode(identity.Name),
				stripUnicode(identity.Email),
				stripUnicode(identity.Username),
			)
			fatalOnError(err)
			sts.identitiesAdded++
		}
	}
	queryStr := ""
	if gProjectSlug == nil {
		if compare {
			queryStr = "select uuid, organization_id, start, end, project_slug from enrollments where uuid = ? and project_slug is null"
		} else {
			queryStr = "select uuid from enrollments where uuid = ? and project_slug is null"
		}
		rows, err = query(db, queryStr, uidentity.UUID)
	} else {
		if compare {
			queryStr = "select uuid, organization_id, start, end, project_slug from enrollments where uuid = ? and project_slug = ?"
		} else {
			queryStr = "select uuid from enrollments where uuid = ? and project_slug = ?"
		}
		rows, err = query(db, queryStr, uidentity.UUID, *gProjectSlug)
	}
	var (
		existingEnrollments []shEnrollment
		existingEnrollment  shEnrollment
	)
	fatalOnError(err)
	fetched = false
	for rows.Next() {
		if compare {
			fatalOnError(
				rows.Scan(
					&existingEnrollment.UUID,
					&existingEnrollment.OrgID,
					&existingEnrollment.Start.Time,
					&existingEnrollment.End.Time,
					&existingEnrollment.ProjectSlug,
				),
			)
			if mtx != nil {
				mtx.RLock()
			}
			organization, ok := id2comp[existingEnrollment.OrgID]
			if mtx != nil {
				mtx.RUnlock()
			}
			if !ok {
				fatalf("organization id %d not found", existingEnrollment.OrgID)
			}
			existingEnrollment.Organization = organization
			existingEnrollments = append(existingEnrollments, existingEnrollment)
		} else {
			fatalOnError(rows.Scan(&uuid))
		}
		fetched = true
		if !compare {
			break
		}
	}
	fatalOnError(rows.Err())
	fatalOnError(rows.Close())
	getCompIds := func() {
		for i, enrollment := range uidentity.Enrollments {
			if mtx != nil {
				mtx.RLock()
			}
			orgID, ok := comp2id[enrollment.Organization]
			if mtx != nil {
				mtx.RUnlock()
			}
			if !ok {
				if orgsRO {
					fmt.Printf("Enrollments: unknown oranization: %s in: %+v\n", enrollment.Organization, uidentity.Enrollments)
					continue
				} else {
					fatalf("organization '%s' not found", enrollment.Organization)
				}
			}
			uidentity.Enrollments[i].OrgID = orgID
		}
	}
	if fetched {
		sts.enrollmentsFound++
	}
	compIDCalculated := false
	same = false
	if fetched && compare {
		getCompIds()
		compIDCalculated = true
		same = !enrollmentsDiffer(uidentity.Enrollments, existingEnrollments)
		if same {
			sts.enrollmentsSame++
		} else if dbg {
			fmt.Printf("Enrollments differ: %+v != %+v\n", uidentity.Enrollments, existingEnrollments)
		}
	}
	if fetched && !same && replace {
		if gProjectSlug == nil {
			_, err := exec(db, "", "delete from enrollments where uuid = ? and project_slug is null", uidentity.UUID)
			fatalOnError(err)
		} else {
			_, err := exec(db, "", "delete from enrollments where uuid = ? and project_slug = ?", uidentity.UUID, *gProjectSlug)
			fatalOnError(err)
		}
		sts.enrollmentsDeleted++
	}
	if !same && (!fetched || (fetched && replace)) {
		if !compIDCalculated {
			getCompIds()
		}
		for _, enrollment := range uidentity.Enrollments {
			if orgsRO && enrollment.OrgID <= 0 {
				sts.enrollmentsSkipped++
				continue
			}
			_, err := exec(
				db,
				"",
				"insert into enrollments(uuid, organization_id, start, end, project_slug) values(?,?,?,?,?)",
				enrollment.UUID,
				enrollment.OrgID,
				enrollment.Start.Time,
				enrollment.End.Time,
				gProjectSlug,
			)
			fatalOnError(err)
			sts.enrollmentsAdded++
		}
	}
	if mtx != nil {
		mtx.Lock()
	}
	stats.uidentitiesAdded += sts.uidentitiesAdded
	stats.uidentitiesFound += sts.uidentitiesFound
	stats.profilesAdded += sts.profilesAdded
	stats.profilesFound += sts.profilesFound
	stats.profilesDeleted += sts.profilesDeleted
	stats.profilesSame += sts.profilesSame
	stats.identitiesAdded += sts.identitiesAdded
	stats.identitiesFound += sts.identitiesFound
	stats.identitiesSame += sts.identitiesSame
	stats.identitiesDeleted += sts.identitiesDeleted
	stats.enrollmentsAdded += sts.enrollmentsAdded
	stats.enrollmentsFound += sts.enrollmentsFound
	stats.enrollmentsSame += sts.enrollmentsSame
	stats.enrollmentsDeleted += sts.enrollmentsDeleted
	stats.enrollmentsSkipped += sts.enrollmentsSkipped
	if mtx != nil {
		mtx.Unlock()
	}
}

func importJSONfiles(db *sql.DB, fileNames []string) error {
	dbg := os.Getenv("DEBUG") != ""
	dry := os.Getenv("DRY") != ""
	replace := os.Getenv("REPLACE") != ""
	compare := os.Getenv("COMPARE") != ""
	projectSlug := os.Getenv("PROJECT_SLUG")
	if projectSlug != "" {
		gProjectSlug = &projectSlug
	}
	orgsRO := os.Getenv("ORGS_RO") != ""
	nFiles := len(fileNames)
	if dbg {
		fmt.Printf("Importing %d files, replace mode: %v\n", nFiles, replace)
	}
	uidentitiesAry := []map[string]shUIdentity{}
	orgs := make(map[string]struct{})
	missingOrgs := make(map[string]struct{})
	countries := make(map[string]*shCountry)
	for i, fileName := range fileNames {
		fmt.Printf("Importing %d/%d: %s\n", i+1, nFiles, fileName)
		var data shData
		contents, err := ioutil.ReadFile(fileName)
		fatalOnError(err)
		fatalOnError(json.Unmarshal(contents, &data))
		fmt.Printf("%s: %d records\n", fileName, len(data.UIdentities))
		for _, uidentity := range data.UIdentities {
			for _, enrollment := range uidentity.Enrollments {
				orgs[enrollment.Organization] = struct{}{}
			}
			if uidentity.Profile.Country != nil {
				code := uidentity.Profile.Country.Code
				_, ok := countries[code]
				if !ok {
					countries[code] = uidentity.Profile.Country
				}
			}
		}
		uidentitiesAry = append(uidentitiesAry, data.UIdentities)
	}
	fmt.Printf("%d orgs present in import files\n", len(orgs))
	comp2id := make(map[string]int)
	id2comp := make(map[int]string)
	lcomp2id := make(map[string]int)
	id2lcomp := make(map[int]string)
	rows, err := query(db, "select id, name from organizations")
	fatalOnError(err)
	orgID := 0
	orgName := ""
	for rows.Next() {
		fatalOnError(rows.Scan(&orgID, &orgName))
		lOrgName := strings.ToLower(orgName)
		comp2id[orgName] = orgID
		id2comp[orgID] = orgName
		lcomp2id[lOrgName] = orgID
		id2lcomp[orgID] = lOrgName
	}
	fatalOnError(rows.Err())
	fatalOnError(rows.Close())
	if dry {
		fmt.Printf("Returing due to dry-run mode\n")
		return nil
	}
	orgsAdded := 0
	orgsMissing := 0
	var (
		exists           bool
		orgNamesMappings allMappings
	)
	thrN := getThreadsNum()
	if orgsRO {
		mut := &sync.RWMutex{}
		orgsLoaded := false
		processOrg := func(ch chan struct{}, comp string) {
			defer func() {
				if ch != nil {
					ch <- struct{}{}
				}
			}()
			mut.RLock()
			cid, exists := comp2id[comp]
			mut.RUnlock()
			if !exists {
				lComp := strings.ToLower(comp)
				mut.RLock()
				_, exists = lcomp2id[lComp]
				mut.RUnlock()
				if !exists {
					mut.RLock()
					if !orgsLoaded {
						mut.RUnlock()
						mut.Lock()
						orgsMap := os.Getenv("ORGS_MAP_FILE")
						if orgsMap != "" {
							var data []byte
							data, err = ioutil.ReadFile(orgsMap)
							fatalOnError(err)
							fatalOnError(yaml.Unmarshal(data, &orgNamesMappings))
						}
						orgsLoaded = true
						mut.Unlock()
					} else {
						mut.RUnlock()
					}
					if dbg {
						fmt.Printf("missing '%s'\n", comp)
					}
					found := false
					for _, mapping := range orgNamesMappings.Mappings {
						re := mapping[0]
						re = strings.Replace(re, "\\\\", "\\", -1)
						if dbg {
							fmt.Printf("check if '%s' matches '%s'\n", comp, re)
						}
						// if comp matches re then to is our mapped company name
						rows, err := query(db, "select ? regexp ?", comp, re)
						fatalOnError(err)
						var m int
						for rows.Next() {
							fatalOnError(rows.Scan(&m))
						}
						fatalOnError(rows.Err())
						fatalOnError(rows.Close())
						if m > 0 {
							if dbg {
								fmt.Printf("'%s' matches '%s'\n", comp, re)
							}
							to := mapping[1]
							mut.RLock()
							cid, exists := comp2id[to]
							mut.RUnlock()
							if exists {
								if dbg {
									fmt.Printf("added mapping '%s' -> '%s' -> %d\n", comp, to, cid)
								}
								mut.Lock()
								comp2id[comp] = cid
								id2comp[cid] = comp
								mut.Unlock()
								found = true
								break
							} else {
								fmt.Printf("'%s' maps to '%s' which cannot be found\n", comp, to)
							}
						} else {
							if dbg {
								fmt.Printf("'%s' is not matching '%s'\n", comp, re)
							}
						}
					}
					if found {
						return
					}
					if dbg {
						fmt.Printf("missing '%s' (trying lower case '%s')\n", comp, lComp)
					}
					for _, mapping := range orgNamesMappings.Mappings {
						re := mapping[0]
						re = strings.Replace(re, "\\\\", "\\", -1)
						if dbg {
							fmt.Printf("check if '%s' matches '%s'\n", lComp, re)
						}
						// if lComp matches re then to is our mapped company name
						rows, err := query(db, "select ? regexp ?", lComp, re)
						fatalOnError(err)
						var m int
						for rows.Next() {
							fatalOnError(rows.Scan(&m))
						}
						fatalOnError(rows.Err())
						fatalOnError(rows.Close())
						if m > 0 {
							if dbg {
								fmt.Printf("'%s' matches '%s'\n", lComp, re)
							}
							to := mapping[1]
							mut.RLock()
							cid, exists := lcomp2id[to]
							mut.RUnlock()
							if exists {
								if dbg {
									fmt.Printf("added mapping '%s' -> '%s' -> %d\n", lComp, to, cid)
								}
								mut.Lock()
								comp2id[comp] = cid
								id2comp[cid] = comp
								mut.Unlock()
								found = true
								break
							} else {
								fmt.Printf("'%s' maps to '%s' which cannot be found\n", lComp, to)
							}
						} else {
							if dbg {
								fmt.Printf("'%s' is not matching '%s'\n", lComp, re)
							}
						}
					}
					if !found {
						fmt.Printf("nothing found for '%s'\n", comp)
						mut.Lock()
						orgsMissing++
						missingOrgs[comp] = struct{}{}
						mut.Unlock()
					}
				} else {
					mut.Lock()
					comp2id[comp] = cid
					id2comp[cid] = comp
					mut.Unlock()
				}
			}
		}
		if thrN > 1 {
			ch := make(chan struct{})
			nThreads := 0
			for org := range orgs {
				go processOrg(ch, org)
				nThreads++
				if nThreads == thrN {
					<-ch
					nThreads--
				}
			}
			for nThreads > 0 {
				<-ch
				nThreads--
			}
		} else {
			for org := range orgs {
				processOrg(nil, org)
			}
		}
	} else {
		for comp := range orgs {
			cid, exists := comp2id[comp]
			if !exists {
				cid, exists = addOrganization(db, comp)
				comp2id[comp] = cid
				id2comp[cid] = comp
			}
			if !exists {
				orgsAdded++
			}
			if dbg {
				fmt.Printf("Org '%s' -> %d\n", comp, cid)
			}
		}
	}
	// fmt.Printf("comp2id:%+v\nod2comp:%+v\n", comp2id, id2comp)
	if len(missingOrgs) > 0 {
		csvFile, err := os.Create(os.Getenv("MISSING_ORGS_CSV"))
		fatalOnError(err)
		defer func() { _ = csvFile.Close() }()
		writer := csv.NewWriter(csvFile)
		fatalOnError(writer.Write([]string{"Organization Name"}))
		for org := range missingOrgs {
			err = writer.Write([]string{org})
		}
		writer.Flush()
	}
	fmt.Printf("Number of organizations: %d, added new: %d, missing: %d\n", len(comp2id), orgsAdded, orgsMissing)
	countriesAdded := 0
	for _, country := range countries {
		exists = addCountry(db, country)
		if !exists {
			countriesAdded++
		}
	}
	fmt.Printf("Number of countries: %d, added new: %d\n", len(countries), countriesAdded)
	var mtx *sync.RWMutex
	if thrN > 1 {
		mtx = &sync.RWMutex{}
	}
	stats := &importStats{}
	for _, uidentities := range uidentitiesAry {
		if thrN > 1 {
			ch := make(chan struct{})
			nThreads := 0
			for _, uidentity := range uidentities {
				go processUIdentity(ch, mtx, db, uidentity, comp2id, id2comp, []bool{dbg, replace, compare, orgsRO}, stats)
				nThreads++
				if nThreads == thrN {
					<-ch
					nThreads--
				}
			}
			for nThreads > 0 {
				<-ch
				nThreads--
			}
		} else {
			for _, uidentity := range uidentities {
				processUIdentity(nil, mtx, db, uidentity, comp2id, id2comp, []bool{dbg, replace, compare, orgsRO}, stats)
			}
		}
	}
	fmt.Printf("Stats:\n%+v\n", stats)
	return nil
}

// getConnectString - get MariaDB SH (Sorting Hat) database DSN
// Either provide full DSN via SH_DSN='shuser:shpassword@tcp(shhost:shport)/shdb?charset=utf8&parseTime=true'
// Or use some SH_ variables, only SH_PASS is required
// Defaults are: "shuser:required_pwd@tcp(localhost:3306)/shdb?charset=utf8
// SH_DSN has higher priority; if set no SH_ varaibles are used
func getConnectString(prefix string) string {
	//dsn := "shuser:"+os.Getenv("PASS")+"@/shdb?charset=utf8")
	dsn := os.Getenv(prefix + "DSN")
	if dsn == "" {
		pass := os.Getenv(prefix + "PASS")
		user := os.Getenv(prefix + "USR")
		if user == "" {
			user = os.Getenv(prefix + "USER")
		}
		proto := os.Getenv(prefix + "PROTO")
		if proto == "" {
			proto = "tcp"
		}
		host := os.Getenv(prefix + "HOST")
		if host == "" {
			host = "localhost"
		}
		port := os.Getenv(prefix + "PORT")
		if port == "" {
			port = "3306"
		}
		db := os.Getenv(prefix + "DB")
		if db == "" {
			fatalf("please specify database via %sDB=...", prefix)
		}
		params := os.Getenv(prefix + "PARAMS")
		if params == "" {
			params = "?charset=utf8&parseTime=true"
		}
		if params == "-" {
			params = ""
		}
		dsn = fmt.Sprintf(
			"%s:%s@%s(%s:%s)/%s%s",
			user,
			pass,
			proto,
			host,
			port,
			db,
			params,
		)
	}
	return dsn
}

func main() {
	// Connect to MariaDB
	if len(os.Args) < 2 {
		fmt.Printf("Arguments required: file.json [file2.json [...]]\n")
		return
	}
	dtStart := time.Now()
	var db *sql.DB
	dsn := getConnectString("SH_")
	db, err := sql.Open("mysql", dsn)
	fatalOnError(err)
	defer func() { fatalOnError(db.Close()) }()
	_, err = db.Exec("set @origin = ?", cOrigin)
	fatalOnError(err)
	err = importJSONfiles(db, os.Args[1:len(os.Args)])
	// Trigger sync event
	/*
		e := ssawsync.Sync(cOrigin)
		if e != nil {
			fmt.Printf("ssaw sync error: %v\n", e)
		}
	*/
	fatalOnError(err)
	dtEnd := time.Now()
	fmt.Printf("Time(%s): %v\n", os.Args[0], dtEnd.Sub(dtStart))
}
