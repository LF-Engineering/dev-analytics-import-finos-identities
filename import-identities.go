package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"runtime/debug"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/yaml.v2"
)

const (
	cOrigin = "import-finos-identities"
	nils    = "(nil)"
)

var (
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
	Others      map[string][]string
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
	s = fmt.Sprintf("{UUID:%s,Organization:%s,OrgID:%d,From:%s,End:%s,ProjectSlug:", e.UUID, e.Organization, e.OrgID, e.Start.String(), e.End.String())
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
	rols = rols[:len(rols)-1] + "]"
	return fmt.Sprintf("{UUID:%s,Profile:%s,Emails:%v,Enrollments:%s,Others:%v}", u.UUID, u.Profile.String(), u.Emails, rols, u.Others)
}

func postprocessIdentities(uidentitiesAry []shUIdentity, unknownsAry []interface{}, uidentitiesMap map[string]shUIdentity) {
	for i, uidentity := range uidentitiesAry {
		if uidentity.Profile.Name == "" {
			fatalf("profile without name: %+v\n", uidentity)
		}
		if len(uidentity.Enrollments) == 0 {
			continue
		}
		iAry, ok := unknownsAry[i].(map[interface{}]interface{})
		if !ok {
			fatalf("cannot parse dynamic datasource identities list fields: %+v\n", uidentity)
		}
		uidentity.Others = make(map[string][]string)
		for ik, iv := range iAry {
			k, ok := ik.(string)
			if !ok {
				fatalf("dynamic datasource identities list - cannot parse key %v,%T as string: %+v\n", ik, ik, uidentity)
			}
			if k == "profile" || k == "enrollments" || k == "email" {
				continue
			}
			v, ok := iv.([]interface{})
			if !ok {
				fatalf("dynamic datasource identities list - cannot parse key %s value %v,%T as array: %+v\n", k, iv, iv, uidentity)
			}
			fmt.Printf("%s,(%v,%v,%v)\n", k, v, ok, iv)
			others := []string{}
			for _, it := range v {
				others = append(others, it.(string))
			}
			uidentity.Others[k] = others
		}
		for ei, enrollment := range uidentity.Enrollments {
			if enrollment.Organization == "" {
				fatalf("enrollment without organization name name: %+v i %+v\n", enrollment, uidentity)
			}
			if enrollment.Start.IsZero() {
				uidentity.Enrollments[ei].Start = gDefaultStartDate
			}
			if enrollment.End.IsZero() {
				uidentity.Enrollments[ei].End = gDefaultEndDate
			}
		}
		// FIXME: find UUID for this uidentity using all data we have here
		fmt.Printf("%s\n", uidentity.String())
		uidentitiesMap[uidentity.UUID] = uidentity
	}
}

func importYAMLfiles(db *sql.DB, fileNames []string) error {
	dbg := os.Getenv("DEBUG") != ""
	//dry := os.Getenv("DRY") != ""
	replace := os.Getenv("REPLACE") != ""
	//compare := os.Getenv("COMPARE") != ""
	projectSlug := os.Getenv("PROJECT_SLUG")
	if projectSlug != "" {
		gProjectSlug = &projectSlug
	}
	nFiles := len(fileNames)
	if dbg {
		fmt.Printf("Importing %d files, replace mode: %v\n", nFiles, replace)
	}
	uidentitiesAry := []map[string]shUIdentity{}
	orgs := make(map[string]struct{})
	//missingOrgs := make(map[string]struct{})
	for i, fileName := range fileNames {
		fmt.Printf("Importing %d/%d: %s\n", i+1, nFiles, fileName)
		var (
			yAry []shUIdentity
			iAry []interface{}
			data shData
		)
		contents, err := ioutil.ReadFile(fileName)
		fatalOnError(err)
		fatalOnError(yaml.Unmarshal(contents, &yAry))
		fatalOnError(yaml.Unmarshal(contents, &iAry))
		data.UIdentities = make(map[string]shUIdentity)
		postprocessIdentities(yAry, iAry, data.UIdentities)
		fmt.Printf("%s: %d records\n", fileName, len(data.UIdentities))
		fmt.Printf("%+v\n", data)
		for _, uidentity := range data.UIdentities {
			for _, enrollment := range uidentity.Enrollments {
				orgs[enrollment.Organization] = struct{}{}
			}
		}
		uidentitiesAry = append(uidentitiesAry, data.UIdentities)
	}
	fmt.Printf("%d orgs present in import files\n", len(orgs))
	/*
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
		orgsMissing := 0
		var (
			exists           bool
			orgNamesMappings allMappings
		)
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
		fmt.Printf("Number of organizations: %d, missing: %d\n", len(comp2id), orgsMissing)
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
	*/
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
