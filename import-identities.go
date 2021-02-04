package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	"gopkg.in/yaml.v2"
)

const (
	cOrigin = "import-finos-identities"
	nils    = "(nil)"
)

var (
	gDebugSQL         bool
	gProjectSlug      *string
	gDefaultStartDate = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	gDefaultEndDate   = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
)

type shData struct {
	UIdentities map[string]shUIdentity
}

type shUIdentity struct {
	Profile     shProfile      `yaml:"profile"`
	Enrollments []shEnrollment `yaml:"enrollments"`
	Emails      []string       `yaml:"email"`
	UUID        string
	Idents      map[string][]string
}

type shProfile struct {
	Name  string `json:"name"`
	IsBot *bool  `json:"is_bot"`
	UUID  string
}

type shEnrollment struct {
	Organization string    `json:"organization"`
	Start        time.Time `json:"start"`
	End          time.Time `json:"end"`
	UUID         string
	OrgID        int
	ProjectSlug  *string
}

type allMappings struct {
	Mappings [][2]string `yaml:"mappings"`
}

type importStats struct {
	uidentitiesFound    int
	uidentitiesNotFound int
	profilesFound       int
	profilesSame        int
	identitiesFound     int
	identitiesSame      int
	enrollmentsFound    int
	enrollmentsAdded    int
	enrollmentsSame     int
	enrollmentsSkipped  int
	enrollmentsDeleted  int
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

func (p *shProfile) String() (s string) {
	s = "{UUID:" + p.UUID + ",Name:" + p.Name
	s += ",IsBot:"
	if p.IsBot != nil {
		s += fmt.Sprintf("%v}", *p.IsBot)
	} else {
		s += nils + "}"
	}
	return
}

func (e *shEnrollment) String() (s string) {
	s = fmt.Sprintf("{UUID:%s,Organization:%s,OrgID:%d,From:%s,End:%s,ProjectSlug:", e.UUID, e.Organization, e.OrgID, toYMDDate(e.Start), toYMDDate(e.End))
	if e.ProjectSlug != nil {
		s += *e.ProjectSlug + "}"
	} else {
		s += nils + "}"
	}
	return
}

func (u *shUIdentity) String() string {
	rols := "["
	for _, rol := range u.Enrollments {
		rols += rol.String() + ","
	}
	if rols != "[" {
		rols = rols[:len(rols)-1] + "]"
	} else {
		rols = "[]"
	}
	return fmt.Sprintf("{UUID:%s,Profile:%s,Emails:%v,Enrollments:%s,Idents:%v}", u.UUID, u.Profile.String(), u.Emails, rols, u.Idents)
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
	if err != nil || gDebugSQL {
		queryOut(query, args...)
	}
	return rows, err
}

func exec(db *sql.DB, skip, query string, args ...interface{}) (sql.Result, error) {
	res, err := db.Exec(query, args...)
	if err != nil || gDebugSQL {
		if skip == "" || !strings.Contains(err.Error(), skip) || gDebugSQL {
			queryOut(query, args...)
		}
	}
	return res, err
}

func getThreadsNum() int {
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

func toYMDDate(dt time.Time) string {
	return fmt.Sprintf("%04d-%02d-%02d", dt.Year(), dt.Month(), dt.Day())
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

func lookupUIdentity(db *sql.DB, dbg bool, uidentity *shUIdentity) (uuid string) {
	printf := func(fmts string, args ...interface{}) {
		if dbg {
			fmt.Printf(fmts, args...)
		}
	}
	name := uidentity.Profile.Name
	// by name
	rows, err := query(db, "select distinct uuid from profiles where name = ?", name)
	fatalOnError(err)
	fetched := false
	multi := false
	for rows.Next() {
		fatalOnError(rows.Scan(&uuid))
		if fetched {
			multi = true
			break
		}
		fetched = true
	}
	fatalOnError(rows.Err())
	fatalOnError(rows.Close())
	if uuid != "" && fetched && !multi {
		printf("found by name '%s' -> %s\n", name, uuid)
		return
	}
	printf("not found by name '%s' -> (%s,%v,%v)\n", name, uuid, fetched, multi)
	// by source/username
	for source, userNames := range uidentity.Idents {
		for _, userName := range userNames {
			rows, err := query(db, "select distinct uuid from identities where username = ? and source = ?", userName, source)
			fatalOnError(err)
			uuid = ""
			fetched = false
			multi = false
			for rows.Next() {
				fatalOnError(rows.Scan(&uuid))
				if fetched {
					multi = true
					break
				}
				fetched = true
			}
			fatalOnError(rows.Err())
			fatalOnError(rows.Close())
			if uuid != "" && fetched && !multi {
				printf("found by source/username '%s/%s' -> %s\n", source, userName, uuid)
				return
			}
			printf("not found by source/username '%s/%s' -> (%s,%v,%v)\n", source, userName, uuid, fetched, multi)
		}
		printf("not found by source/usernames '%s/%v' -> (%s,%v,%v)\n", source, userNames, uuid, fetched, multi)
	}
	// by email
	for _, email := range uidentity.Emails {
		rows, err := query(db, "select distinct uuid from identities where email = ?", email)
		fatalOnError(err)
		uuid = ""
		fetched = false
		multi = false
		for rows.Next() {
			fatalOnError(rows.Scan(&uuid))
			if fetched {
				multi = true
				break
			}
			fetched = true
		}
		fatalOnError(rows.Err())
		fatalOnError(rows.Close())
		if uuid != "" && fetched && !multi {
			printf("found by email '%s' -> %s\n", email, uuid)
			return
		}
		printf("not found by email '%s' -> (%s,%v,%v)\n", email, uuid, fetched, multi)
	}
	// by name & source/username
	for source, userNames := range uidentity.Idents {
		for _, userName := range userNames {
			rows, err := query(db, "select distinct uuid from identities where name = ? and username = ? and source = ?", name, userName, source)
			fatalOnError(err)
			uuid = ""
			fetched = false
			multi = false
			for rows.Next() {
				fatalOnError(rows.Scan(&uuid))
				if fetched {
					multi = true
					break
				}
				fetched = true
			}
			fatalOnError(rows.Err())
			fatalOnError(rows.Close())
			if uuid != "" && fetched && !multi {
				printf("found by name/source/username '%s/%s/%s' -> %s\n", name, source, userName, uuid)
				return
			}
			printf("not found by name/source/username '%s/%s/%s' -> (%s,%v,%v)\n", name, source, userName, uuid, fetched, multi)
		}
		printf("not found by name/source/usernames '%s/%s/%v' -> (%s,%v,%v)\n", name, source, userNames, uuid, fetched, multi)
	}
	// by name & email
	for _, email := range uidentity.Emails {
		rows, err := query(db, "select distinct uuid from identities where name = ? and email = ?", name, email)
		fatalOnError(err)
		uuid = ""
		fetched = false
		multi = false
		for rows.Next() {
			fatalOnError(rows.Scan(&uuid))
			if fetched {
				multi = true
				break
			}
			fetched = true
		}
		fatalOnError(rows.Err())
		fatalOnError(rows.Close())
		if uuid != "" && fetched && !multi {
			printf("found by name/email '%s/%s' -> %s\n", name, email, uuid)
			return
		}
		printf("not found by name/email '%s/%s' -> (%s,%v,%v)\n", name, email, uuid, fetched, multi)
	}
	// by source/username/email
	for source, userNames := range uidentity.Idents {
		for _, email := range uidentity.Emails {
			for _, userName := range userNames {
				rows, err := query(db, "select distinct uuid from identities where username = ? and source = ? and email = ?", userName, source, email)
				fatalOnError(err)
				uuid = ""
				fetched = false
				multi = false
				for rows.Next() {
					fatalOnError(rows.Scan(&uuid))
					if fetched {
						multi = true
						break
					}
					fetched = true
				}
				fatalOnError(rows.Err())
				fatalOnError(rows.Close())
				if uuid != "" && fetched && !multi {
					printf("found by email/source/username '%s/%s/%s' -> %s\n", email, source, userName, uuid)
					return
				}
				printf("not found by email/source/username '%s/%s/%s' -> (%s,%v,%v)\n", email, source, userName, uuid, fetched, multi)
			}
			printf("not found by email/source/usernames '%s/%s/%v' -> (%s,%v,%v)\n", email, source, userNames, uuid, fetched, multi)
		}
		printf("not found by emails/source/usernames '%sv/%s/%v' -> (%s,%v,%v)\n", uidentity.Emails, source, userNames, uuid, fetched, multi)
	}
	// by name/source/username/email
	for source, userNames := range uidentity.Idents {
		for _, email := range uidentity.Emails {
			for _, userName := range userNames {
				rows, err := query(db, "select distinct uuid from identities where username = ? and source = ? and email = ? and name = ?", userName, source, email, name)
				fatalOnError(err)
				uuid = ""
				fetched = false
				multi = false
				for rows.Next() {
					fatalOnError(rows.Scan(&uuid))
					if fetched {
						multi = true
						break
					}
					fetched = true
				}
				fatalOnError(rows.Err())
				fatalOnError(rows.Close())
				if uuid != "" && fetched && !multi {
					printf("found by name/email/source/username '%s/%s/%s/%s' -> %s\n", name, email, source, userName, uuid)
					return
				}
				printf("not found by name/email/source/username '%s/%s/%s/%s' -> (%s,%v,%v)\n", name, email, source, userName, uuid, fetched, multi)
			}
			printf("not found by name/email/source/usernames '%s/%s/%s/%v' -> (%s,%v,%v)\n", name, email, source, userNames, uuid, fetched, multi)
		}
		printf("not found by name/emails/source/usernames '%s/%v/%s/%v' -> (%s,%v,%v)\n", name, uidentity.Emails, source, userNames, uuid, fetched, multi)
	}
	uuid = ""
	return
}

func postprocessIdentities(db *sql.DB, dbg bool, uidentitiesAry []shUIdentity, unknownsAry []interface{}, uidentitiesMap map[string]shUIdentity) (missing []shUIdentity) {
	fmt.Printf("processing %d profiles\n", len(uidentitiesAry))
	setUUID := func(uident *shUIdentity, uid string) {
		uident.UUID = uid
		uident.Profile.UUID = uid
		for i := range uident.Enrollments {
			uident.Enrollments[i].UUID = uid
			uident.Enrollments[i].ProjectSlug = gProjectSlug
		}
	}
	type resultType struct {
		i    int
		uuid string
	}
	var mtx *sync.Mutex
	processItem := func(ch chan resultType, idx int, uidentity shUIdentity) (result resultType) {
		uuid := ""
		result.i = idx
		defer func() {
			result.uuid = uuid
			if ch != nil {
				ch <- result
			}
		}()
		if uidentity.Profile.Name == "" {
			fatalf("profile without name: %+v\n", uidentity.String())
		}
		if len(uidentity.Enrollments) == 0 {
			uuid = "skip"
			return
		}
		iAry, ok := unknownsAry[idx].(map[interface{}]interface{})
		if !ok {
			fatalf("cannot parse dynamic datasource identities list fields: %+v\n", uidentity.String())
		}
		uidentity.Idents = make(map[string][]string)
		for ik, iv := range iAry {
			k, ok := ik.(string)
			if !ok {
				fatalf("dynamic datasource identities list - cannot parse key %v,%T as string: %+v\n", ik, ik, uidentity.String())
			}
			if k == "profile" || k == "enrollments" || k == "email" {
				continue
			}
			v, ok := iv.([]interface{})
			if !ok {
				fatalf("dynamic datasource identities list - cannot parse key %s value %v,%T as array: %+v\n", k, iv, iv, uidentity.String())
			}
			others := []string{}
			for _, it := range v {
				its, ok := it.(string)
				if !ok {
					fatalf("dynamic datasource identities list - cannot parse key %s value %v item %v,%v as string: %+v\n", k, v, it, it, uidentity.String())
					continue
				}
				others = append(others, its)
			}
			uidentity.Idents[k] = others
		}
		for ei, enrollment := range uidentity.Enrollments {
			if enrollment.Organization == "" {
				fatalf("enrollment without organization name: %+v in %+v\n", enrollment.String(), uidentity.String())
			}
			if enrollment.Start.IsZero() {
				uidentity.Enrollments[ei].Start = gDefaultStartDate
			}
			if enrollment.End.IsZero() {
				uidentity.Enrollments[ei].End = gDefaultEndDate
			}
		}
		if mtx != nil {
			mtx.Lock()
		}
		uidentitiesAry[idx].Idents = uidentity.Idents
		if mtx != nil {
			mtx.Unlock()
		}
		uuid = lookupUIdentity(db, dbg, &uidentity)
		if uuid == "" {
			if dbg {
				fmt.Printf("WARNING: cannot find %s identity in our database\n", uidentity.String())
			}
			return
		}
		if dbg {
			fmt.Printf("found %s\n", uidentity.String())
		}
		return
	}
	processResult := func(result resultType) {
		idx := result.i
		uuid := result.uuid
		if uuid == "skip" {
			return
		}
		if uuid != "" {
			setUUID(&uidentitiesAry[idx], uuid)
			uidentitiesMap[uuid] = uidentitiesAry[idx]
		} else {
			missing = append(missing, uidentitiesAry[idx])
		}
	}
	thrN := getThreadsNum()
	ch := make(chan resultType)
	if thrN > 1 {
		nThreads := 0
		mtx = &sync.Mutex{}
		for i, uidentity := range uidentitiesAry {
			go processItem(ch, i, uidentity)
			nThreads++
			if nThreads == thrN {
				processResult(<-ch)
				nThreads--
			}
		}
		for nThreads > 0 {
			processResult(<-ch)
			nThreads--
		}
	} else {
		for i, uidentity := range uidentitiesAry {
			processResult(processItem(nil, i, uidentity))
		}
	}
	if len(missing) > 0 {
		fmt.Printf("cannot find %d profiles\n", len(missing))
	}
	return
}

func cleanupUnaffiliated(dbg bool, uidentities []shUIdentity) {
	// Remove: Unaffiliated
	// Possibly: Individual Contributor
	for i, uidentity := range uidentities {
		for j, enrollment := range uidentity.Enrollments {
			if enrollment.Organization == "Unaffiliated" {
				last := len(uidentities[i].Enrollments) - 1
				if last == 0 {
					uidentities[i].Enrollments = []shEnrollment{}
					if dbg {
						fmt.Printf("removed %s enrollment: no enrollments left: %s\n", enrollment.Organization, uidentities[i].String())
					}
					continue
				}
				uidentities[i].Enrollments[j] = uidentities[i].Enrollments[last]
				uidentities[i].Enrollments = uidentities[i].Enrollments[:last]
				if dbg {
					fmt.Printf("removed %s enrollment: new enrollments: %v: %s\n", enrollment.Organization, uidentities[i].Enrollments, uidentities[i].String())
				}
			}
		}
	}
}

func importYAMLfiles(db *sql.DB, fileNames []string) error {
	dbg := os.Getenv("DEBUG") != ""
	dry := os.Getenv("DRY") != ""
	replace := os.Getenv("REPLACE") != ""
	compare := os.Getenv("COMPARE") != ""
	projectSlug := os.Getenv("PROJECT_SLUG")
	if projectSlug != "" {
		gProjectSlug = &projectSlug
	}
	gDebugSQL = os.Getenv("DEBUG_SQL") != ""
	nFiles := len(fileNames)
	if dbg {
		fmt.Printf("importing %d files, debug: %v, dry-run: %v, compare mode: %v, replace mode: %v\n", nFiles, dbg, dry, compare, replace)
	}
	uidentitiesAry := []map[string]shUIdentity{}
	orgs := make(map[string]struct{})
	missingOrgs := make(map[string]struct{})
	missingProfiles := []shUIdentity{}
	timeSuff := func() string {
		dt := time.Now()
		return fmt.Sprintf("_%04d%02d%02d%02d%02d%02d%09d", dt.Year(), dt.Month(), dt.Day(), dt.Hour(), dt.Minute(), dt.Second(), dt.Nanosecond())
	}
	for i, fileName := range fileNames {
		fmt.Printf("importing %d/%d: %s\n", i+1, nFiles, fileName)
		var (
			yAry []shUIdentity
			iAry []interface{}
			data shData
		)
		contents, err := ioutil.ReadFile(fileName)
		fatalOnError(err)
		fatalOnError(yaml.Unmarshal(contents, &yAry))
		fatalOnError(yaml.Unmarshal(contents, &iAry))
		cleanupUnaffiliated(dbg, yAry)
		data.UIdentities = make(map[string]shUIdentity)
		missing := postprocessIdentities(db, dbg, yAry, iAry, data.UIdentities)
		for _, miss := range missing {
			missingProfiles = append(missingProfiles, miss)
		}
		fmt.Printf("%s: %d records\n", fileName, len(data.UIdentities))
		for _, uidentity := range data.UIdentities {
			for _, enrollment := range uidentity.Enrollments {
				orgs[enrollment.Organization] = struct{}{}
			}
		}
		uidentitiesAry = append(uidentitiesAry, data.UIdentities)
	}
	if len(missingProfiles) > 0 {
		fn := os.Getenv("MISSING_PROFILES_CSV")
		if fn == "" {
			fn = "missing_profiles"
		}
		csvFile, err := os.Create(fn + timeSuff() + ".csv")
		fatalOnError(err)
		defer func() { _ = csvFile.Close() }()
		writer := csv.NewWriter(csvFile)
		fatalOnError(writer.Write([]string{"Name", "Emails", "Identities", "Enrollments"}))
		for _, uidentity := range missingProfiles {
			rols := ""
			for _, rol := range uidentity.Enrollments {
				rols += rol.Organization
				if rol.Start.After(gDefaultStartDate) {
					rols += " from:" + toYMDDate(rol.Start)
				}
				if rol.End.Before(gDefaultEndDate) {
					rols += " to:" + toYMDDate(rol.End)
				}
				rols += ","
			}
			if rols != "" {
				rols = rols[:len(rols)-1]
			}
			idents := ""
			for source, userNames := range uidentity.Idents {
				idents += source + ": ["
				for _, userName := range userNames {
					idents += userName + ","
				}
				idents = idents[:len(idents)-1] + "],"
			}
			if idents != "" {
				idents = idents[:len(idents)-1]
			}
			err = writer.Write(
				[]string{
					uidentity.Profile.Name,
					strings.Join(uidentity.Emails, ","),
					idents,
					rols,
				},
			)
		}
		writer.Flush()
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
	//fmt.Printf("comp2id: %+v\n", comp2id)
	//fmt.Printf("id2comp: %+v\n", id2comp)
	//fmt.Printf("lcomp2id: %+v\n", lcomp2id)
	//fmt.Printf("id2lcomp: %+v\n", id2lcomp)
	orgsMissing := 0
	var orgNamesMappings allMappings
	thrN := getThreadsNum()
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
							// Consider
							id2comp[cid] = comp
							//id2comp[cid] = to
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
							// Consider
							id2comp[cid] = comp
							// id2comp[cid] = to
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
	// fmt.Printf("comp2id:%+v\nod2comp:%+v\n", comp2id, id2comp)
	if len(missingOrgs) > 0 {
		fn := os.Getenv("MISSING_ORGS_CSV")
		if fn == "" {
			fn = "missing_orgs"
		}
		csvFile, err := os.Create(fn + timeSuff() + ".csv")
		fatalOnError(err)
		defer func() { _ = csvFile.Close() }()
		writer := csv.NewWriter(csvFile)
		fatalOnError(writer.Write([]string{"Organization Name"}))
		for org := range missingOrgs {
			err = writer.Write([]string{org})
		}
		writer.Flush()
	}
	if dbg {
		fmt.Printf("comp2id: %+v\n", comp2id)
		fmt.Printf("id2comp: %+v\n", id2comp)
		fmt.Printf("lcomp2id: %+v\n", lcomp2id)
		fmt.Printf("id2lcomp: %+v\n", id2lcomp)
	}
	fmt.Printf("Number of organizations: %d, missing: %d\n", len(comp2id), orgsMissing)
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
				go processUIdentity(ch, mtx, db, uidentity, comp2id, id2comp, []bool{dbg, replace, compare}, stats)
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
				processUIdentity(nil, mtx, db, uidentity, comp2id, id2comp, []bool{dbg, replace, compare}, stats)
			}
		}
	}
	fmt.Printf("Stats:\n%+v\n", stats)
	return nil
}

func profilesDiffer(p1, p2 *shProfile) bool {
	if stripUnicodeStr(p1.Name) != stripUnicodeStr(p2.Name) {
		return true
	}
	if p1.IsBot != nil && p2.IsBot != nil && *p1.IsBot != *p2.IsBot {
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
	var sts importStats
	defer func() {
		if mtx != nil {
			mtx.Lock()
		}
		stats.uidentitiesFound += sts.uidentitiesFound
		stats.uidentitiesNotFound += sts.uidentitiesNotFound
		stats.profilesFound += sts.profilesFound
		stats.profilesSame += sts.profilesSame
		stats.identitiesFound += sts.identitiesFound
		stats.identitiesSame += sts.identitiesSame
		stats.enrollmentsFound += sts.enrollmentsFound
		stats.enrollmentsSame += sts.enrollmentsSame
		stats.enrollmentsAdded += sts.enrollmentsAdded
		stats.enrollmentsSkipped += sts.enrollmentsSkipped
		stats.enrollmentsDeleted += sts.enrollmentsDeleted
		if mtx != nil {
			mtx.Unlock()
		}
	}()
	_, _ = db.Exec("set @origin = ?", cOrigin)
	dbg := flags[0]
	replace := flags[1]
	compare := flags[2]
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
		fmt.Printf("cannot find uidentity '%s'\n", uidentity.UUID)
		sts.uidentitiesNotFound++
		return
	}
	sts.uidentitiesFound++
	var existingProfile shProfile
	rows, err = query(
		db,
		"select uuid, coalesce(name, ''), is_bot from profiles where uuid = ?",
		uidentity.UUID,
	)
	fatalOnError(err)
	fetched = false
	for rows.Next() {
		fatalOnError(
			rows.Scan(
				&existingProfile.UUID,
				&existingProfile.Name,
				&existingProfile.IsBot,
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
		same = !profilesDiffer(&uidentity.Profile, &existingProfile)
		if same {
			sts.profilesSame++
		} else if dbg {
			fmt.Printf("Profiles differ: %s != %s\n", uidentity.Profile.String(), existingProfile.String())
		}
	}
	emails := make(map[string]struct{})
	for _, email := range uidentity.Emails {
		emails[stripUnicodeStr(email)] = struct{}{}
	}
	if len(emails) > 0 && compare {
		for source, userNames := range uidentity.Idents {
			for _, userName := range userNames {
				eemail := ""
				rows, err = query(
					db,
					"select coalesce(email, '') from identities where uuid = ? and source = ? and username = ? and email is not null",
					uidentity.UUID,
					source,
					stripUnicodeStr(userName),
				)
				fatalOnError(err)
				fetched = false
				for rows.Next() {
					fatalOnError(rows.Scan(&eemail))
					eemail = stripUnicodeStr(eemail)
					fetched = true
					break
				}
				fatalOnError(rows.Err())
				fatalOnError(rows.Close())
				if fetched {
					sts.identitiesFound++
				}
				same = false
				if fetched {
					_, ok := emails[eemail]
					if ok {
						sts.identitiesSame++
					} else if dbg {
						fmt.Printf("Identities differ uuid: %s source: %s, username: %s, email %s not in %v\n", uidentity.UUID, source, userName, eemail, emails)
					}
				}
			}
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
					&existingEnrollment.Start,
					&existingEnrollment.End,
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
				fmt.Printf("Enrollments: unknown oranization: %s in: %+v\n", enrollment.Organization, uidentity.Enrollments)
				continue
			}
			uidentity.Enrollments[i].OrgID = orgID
			if mtx != nil {
				mtx.RLock()
			}
			org, ok := id2comp[orgID]
			if mtx != nil {
				mtx.RUnlock()
			}
			if !ok {
				continue
			}
			if org != enrollment.Organization {
				if dbg {
					fmt.Printf("updaing org name that would be mapped: '%s' -> '%s'\n", enrollment.Organization, org)
				}
				uidentity.Enrollments[i].Organization = org
			}
		}
	}
	if fetched {
		sts.enrollmentsFound++
	}
	rolsString := func(rols []shEnrollment) string {
		ary := []string{}
		for _, rol := range rols {
			ary = append(ary, rol.String())
		}
		if len(ary) == 0 {
			return ""
		}
		if len(ary) > 1 {
			sort.Strings(ary)
		}
		return "[" + strings.Join(ary, ",") + "]"
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
			fmt.Printf("Enrollments differ: %+v != %+v\n", rolsString(uidentity.Enrollments), rolsString(existingEnrollments))
		}
	}
	// found, they differ (or compare mode is off) and replace mode is on
	// delete them
	// fmt.Printf("state (%v,%v,%v,%v)\n", fetched, same, compare, replace)
	if fetched && !same && replace {
		if dbg {
			fmt.Printf("deleting enrollments for %s/%s\n", uidentity.UUID, *gProjectSlug)
		}
		if gProjectSlug == nil {
			_, err := exec(db, "", "delete from enrollments where uuid = ? and project_slug is null", uidentity.UUID)
			fatalOnError(err)
		} else {
			_, err := exec(db, "", "delete from enrollments where uuid = ? and project_slug = ?", uidentity.UUID, *gProjectSlug)
			fatalOnError(err)
		}
		sts.enrollmentsDeleted++
	}
	// they differ (which means there are no rols, compare mode is off or they actually differ) and
	// none fetched or some fetched and replace mode is on
	// add them
	if !same && (!fetched || (fetched && replace)) {
		if dbg {
			fmt.Printf("adding enrollments for %s/%s\n", uidentity.UUID, *gProjectSlug)
		}
		if !compIDCalculated {
			getCompIds()
		}
		for _, enrollment := range uidentity.Enrollments {
			if enrollment.OrgID <= 0 {
				sts.enrollmentsSkipped++
				continue
			}
			if dbg {
				fmt.Printf("adding enrollment for %s/%s/%s\n", uidentity.UUID, *gProjectSlug, enrollment.String())
			}
			_, err := exec(
				db,
				"",
				"insert into enrollments(uuid, organization_id, start, end, project_slug) values(?,?,?,?,?)",
				enrollment.UUID,
				enrollment.OrgID,
				enrollment.Start,
				enrollment.End,
				gProjectSlug,
			)
			fatalOnError(err)
			sts.enrollmentsAdded++
		}
	}
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
		fmt.Printf("Arguments required: file.yaml\n")
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
	err = importYAMLfiles(db, os.Args[1:len(os.Args)])
	fatalOnError(err)
	dtEnd := time.Now()
	fmt.Printf("Time(%s): %v\n", os.Args[0], dtEnd.Sub(dtStart))
}
