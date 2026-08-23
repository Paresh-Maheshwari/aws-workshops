// awsworkshops — a single-file Go program that harvests the AWS Experience event
// API across every regional portal, keeps only workshops that are both ONLINE and
// HANDS-ON, converts every time to IST, and serves a filterable page.
//
//	go build -o awsworkshops main.go
//	./awsworkshops                 # serves http://127.0.0.1:8765
//	./awsworkshops -port 9000 -open
//
// No dependencies beyond the standard library. No template, asset or data files —
// the UI and the timezone database are compiled into the binary.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "time/tzdata" // embed the IANA database so Asia/Kolkata resolves anywhere
)

// ---------------------------------------------------------------- config

// Portals confirmed to serve event JSON. Established by brute-force probing 336
// region/slug combinations, cross-checked against the /emea landing page nav.
var portals = []string{
	"amer/smb",
	"emea/smb",
	"emea/de-central-growth",
	"emea/uki",
	"emea/north",
	"emea/italia",
	"emea/iberia",
	"emea/tel-aviv",
	"emea/france",
	"apj/smb",
	"latam/smb",
}

// The two endpoints that carry event records. "externalevent" entries bring their
// own off-site registrationUrl.
var endpoints = []string{"session", "externalevent"}

const userAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"

var regionNames = map[string]string{
	"amer": "AMER", "emea": "EMEA", "apj": "APJ", "latam": "LATAM",
}

// ---------------------------------------------------------------- API shapes

type apiSpeaker struct {
	Name string `json:"name"`
}

type apiSetting struct {
	Setting string `json:"setting"`
	Details struct {
		HostingPlatform string      `json:"hostingPlatform"`
		Capacity        json.Number `json:"capacity"`
	} `json:"details"`
}

type apiEvent struct {
	ID                string       `json:"id"`
	Title             string       `json:"title"`
	Summary           string       `json:"summary"`
	Type              string       `json:"type"`
	Status            string       `json:"status"`
	StartInUtc        string       `json:"startInUtc"`
	EndInUtc          string       `json:"endInUtc"`
	StartWithTimeZone string       `json:"startWithTimeZone"`
	TimeZone          string       `json:"timeZone"`
	Levels            []string     `json:"levels"`
	Language          string       `json:"language"`
	Series            []string     `json:"series"`
	Speakers          []apiSpeaker `json:"speakers"`
	Settings          []apiSetting `json:"settingDetails"`
	URLSlug           string       `json:"urlSlug"`
	RegistrationURL   string       `json:"registrationUrl"`
}

type apiResp struct {
	Future []apiEvent `json:"future"`
	Past   []apiEvent `json:"past"`
}

// ---------------------------------------------------------------- output shape

// Workshop is what the page consumes. Instants stay in UTC: the page derives
// every date and clock time from them in the reader's chosen zone.
type Workshop struct {
	ID     string `json:"id"`
	Title  string `json:"title"`
	URL    string `json:"url"`
	NoPage bool   `json:"nopage,omitempty"`
	Region string `json:"region"`
	Portal string `json:"portal"`

	// Both instants are RFC3339 UTC. Every date and clock time the page displays
	// is derived from these in the browser, so the reader can switch timezone
	// without a rebuild. Dur is a span, not an instant, so it stays precomputed.
	Start string `json:"start"`
	End   string `json:"end,omitempty"`
	Dur   string `json:"dur"`

	// The organiser's own clock, parsed from the offset the API supplies so no
	// IANA lookup is needed. LocalDate lets the page decide whether that date
	// needs spelling out, which depends on the zone the reader picked.
	LocalTime string `json:"localTime"`
	LocalDate string `json:"localDate,omitempty"`
	TZ        string `json:"tz"`

	Platform string `json:"platform"`
	Capacity string `json:"capacity,omitempty"`
	Levels   string `json:"levels"`
	Language string `json:"language"`
	Series   string `json:"series"`
	Speakers string `json:"speakers"`
	Summary  string `json:"summary"`
}

// ---------------------------------------------------------------- store

type store struct {
	mu        sync.RWMutex
	workshops []Workshop
	built     time.Time
	warnings  []string
}

func (s *store) set(w []Workshop, warn []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workshops, s.built, s.warnings = w, time.Now(), warn
}

func (s *store) get() ([]Workshop, time.Time, []string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.workshops, s.built, s.warnings
}

// ---------------------------------------------------------------- harvest

var ist *time.Location

func fetchOne(ctx context.Context, cl *http.Client, portal, ep string) (*apiResp, error) {
	url := "https://aws-experience.com/" + portal + "/api/" + ep
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 2 * time.Second) // the origin throttles bursts
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("accept", "application/json")
		req.Header.Set("user-agent", userAgent)
		req.Header.Set("referer", "https://aws-experience.com/"+portal+"/events")

		resp, err := cl.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			continue
		}
		var out apiResp
		if err := json.Unmarshal(body, &out); err != nil {
			// some endpoints return a JSON array rather than {future,past}
			lastErr = fmt.Errorf("decode: %w", err)
			continue
		}
		return &out, nil
	}
	return nil, lastErr
}

// harvest pulls every portal/endpoint pair, keeping concurrency low enough that
// the origin does not throttle us into empty responses.
func harvest(ctx context.Context) ([]Workshop, []string) {
	cl := &http.Client{Timeout: 60 * time.Second}

	type job struct{ portal, ep string }
	jobs := make([]job, 0, len(portals)*len(endpoints))
	for _, p := range portals {
		for _, e := range endpoints {
			jobs = append(jobs, job{p, e})
		}
	}

	var (
		mu  sync.Mutex
		raw []struct {
			portal string
			ev     apiEvent
		}
		warnings []string
		wg       sync.WaitGroup
		sem      = make(chan struct{}, 3)
	)

	for _, j := range jobs {
		wg.Add(1)
		go func(j job) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			time.Sleep(200 * time.Millisecond) // pace

			r, err := fetchOne(ctx, cl, j.portal, j.ep)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				warnings = append(warnings, j.portal+"/"+j.ep+": "+err.Error())
				return
			}
			for _, ev := range append(r.Future, r.Past...) {
				raw = append(raw, struct {
					portal string
					ev     apiEvent
				}{j.portal, ev})
			}
		}(j)
	}
	wg.Wait()

	now := time.Now().UTC()
	seen := make(map[string]bool, len(raw))
	out := make([]Workshop, 0, 512)

	for _, item := range raw {
		w, ok := convert(item.ev, item.portal, now)
		if !ok {
			continue
		}
		if seen[w.ID] {
			// Guard only. Verified against 827 harvested records: every event carries a
			// unique id, so this currently removes nothing. It is NOT safe to de-duplicate
			// on title, or even title+start — 56 titles run on several portals and 7
			// title+start pairs are backed by two different ids, each a genuinely separate
			// session with its own slug and speaker roster.
			continue
		}
		seen[w.ID] = true
		out = append(out, w)
	}

	// sort.Slice is not stable, so tie-break explicitly: events that share a start
	// time would otherwise shuffle between harvests
	sort.Slice(out, func(i, j int) bool {
		if out[i].Start != out[j].Start {
			return out[i].Start < out[j].Start
		}
		if out[i].Title != out[j].Title {
			return out[i].Title < out[j].Title
		}
		return out[i].ID < out[j].ID
	})
	sort.Strings(warnings)
	return out, warnings
}

// convert applies the "online AND hands-on AND live AND upcoming" rule and
// resolves every timestamp to IST. It reports false when the event is filtered out.
func convert(e apiEvent, portal string, now time.Time) (Workshop, bool) {
	var zero Workshop

	// hands-on only. The API is inconsistent about casing.
	if !strings.EqualFold(e.Type, "handsonworkshop") {
		return zero, false
	}
	if !strings.EqualFold(e.Status, "live") {
		return zero, false
	}

	// online only
	var platform, capacity string
	online := false
	for _, s := range e.Settings {
		if strings.EqualFold(s.Setting, "virtual") {
			online = true
			platform = s.Details.HostingPlatform
			if c := s.Details.Capacity.String(); c != "" && c != "0" {
				capacity = c
			}
			break
		}
	}
	if !online {
		return zero, false
	}

	start, err := time.ParseInLocation("2006-01-02T15:04", e.StartInUtc, time.UTC)
	if err != nil || !start.After(now) {
		return zero, false
	}

	w := Workshop{
		ID:       e.ID,
		Title:    strings.TrimSpace(e.Title),
		Region:   regionNames[strings.SplitN(portal, "/", 2)[0]],
		Portal:   portal,
		Start:    start.Format(time.RFC3339),
		Language: e.Language,
		Levels:   strings.Join(e.Levels, "/"),
		Series:   strings.Join(e.Series, "; "),
		Platform: platform,
		Capacity: capacity,
		Summary:  clip(collapse(e.Summary), 260),
		TZ:       strings.ReplaceAll(e.TimeZone, "_", " "),
	}

	if end, err := time.ParseInLocation("2006-01-02T15:04", e.EndInUtc, time.UTC); err == nil {
		if d := end.Sub(start); d > 0 && d < 72*time.Hour {
			w.End = end.Format(time.RFC3339)
			w.Dur = humanDur(d)
		}
	}

	// the organiser's own clock. The page spells the date out only when it differs
	// from the date in whichever zone the reader is viewing.
	if lt, err := time.Parse("2006-01-02T15:04Z07:00", e.StartWithTimeZone); err == nil {
		w.LocalTime = lt.Format("15:04")
		w.LocalDate = lt.Format("2006-01-02")
	}

	names := make([]string, 0, len(e.Speakers))
	for _, s := range e.Speakers {
		if n := strings.TrimSpace(s.Name); n != "" {
			names = append(names, n)
		}
	}
	w.Speakers = clip(strings.Join(names, "; "), 140)

	// URL: /e/<slug> is the dedicated page. /events/<slug> returns 200 but renders
	// the generic calendar, so it must not be used.
	switch {
	case e.RegistrationURL != "":
		w.URL = e.RegistrationURL
	case e.URLSlug != "":
		w.URL = "https://aws-experience.com/" + portal + "/e/" + e.URLSlug
	default:
		// ~66 records carry no slug, so no dedicated page exists
		w.URL = "https://aws-experience.com/" + portal + "/events"
		w.NoPage = true
	}
	return w, true
}

func humanDur(d time.Duration) string {
	m := int(d.Minutes())
	switch {
	case m < 60:
		return fmt.Sprintf("%dm", m)
	case m%60 == 0:
		return fmt.Sprintf("%dh", m/60)
	default:
		return fmt.Sprintf("%dh %dm", m/60, m%60)
	}
}

func collapse(s string) string { return strings.Join(strings.Fields(s), " ") }

// ---------------------------------------------------------------- page render

// Server-only chrome. A static build has no /refresh or /healthz behind it, so
// advertising them would hand the reader three dead links.
const footServer = `<section>
        <h2 class="foothd">This server</h2>
        <ul class="footlist">
          <li><a href="/api/workshops">/api/workshops</a> &mdash; the same data as JSON</li>
          <li><a href="/healthz">/healthz</a> &mdash; count, build time, harvest warnings</li>
          <li><a href="/refresh">/refresh</a> &mdash; re-harvest immediately</li>
        </ul>
      </section>`

const footStatic = `<section>
        <h2 class="foothd">This page</h2>
        <p>A static snapshot &mdash; the harvest runs on a schedule and rebuilds this
          page, so the times above are fixed until the next run. The listing is baked
          into the page itself, so nothing is fetched when you load it.</p>
      </section>`

// renderPage injects the harvested data into the template. static drops every
// piece of chrome that needs an origin behind it: the refresh button, the JSON
// link, and the endpoint list in the footer.
func renderPage(list []Workshop, built time.Time, static bool) string {
	data, err := json.Marshal(list)
	if err != nil {
		data = []byte("[]")
	}

	refreshBtn := `<button class="chip" type="button" id="refresh">Refresh</button>`
	jsonLink := `<a class="mastlink" href="/api/workshops" target="_blank" rel="noopener">JSON</a>`
	footSection := footServer
	if static {
		refreshBtn = "" // no endpoint to call
		jsonLink = ""   // the page is the only artefact a static build ships
		footSection = footStatic
	}

	// NewReplacer makes a single left-to-right pass and does not rescan what it
	// inserts, so harvested text can never be mistaken for another placeholder.
	return strings.NewReplacer(
		"__DATA__", string(data),
		"__COUNT__", fmt.Sprint(len(list)),
		"__BUILT__", built.In(ist).Format("Mon 2 Jan 2006, 15:04 IST"),
		"__BUILT_ISO__", built.UTC().Format(time.RFC3339),
		"__REFRESHBTN__", refreshBtn,
		"__JSONLINK__", jsonLink,
		"__FOOTSERVER__", footSection,
	).Replace(pageHTML)
}

// renderRSS generates an RSS 2.0 feed with the most recent workshops.
// Limit to 50 items to keep the feed size reasonable.
func renderRSS(list []Workshop, built time.Time) string {
	const siteURL = "https://paresh-maheshwari.github.io/aws-workshops/"
	const feedURL = "https://paresh-maheshwari.github.io/aws-workshops/feed.xml"

	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">
<channel>
  <title>AWS Online Hands-on Workshops</title>
  <link>` + siteURL + `</link>
  <description>Upcoming virtual hands-on workshops from AWS Experience portals across AMER, EMEA, APJ, and LATAM.</description>
  <language>en</language>
  <lastBuildDate>` + built.UTC().Format(time.RFC1123Z) + `</lastBuildDate>
  <atom:link href="` + feedURL + `" rel="self" type="application/rss+xml"/>
`)

	// Limit to 50 items
	n := len(list)
	if n > 50 {
		n = 50
	}

	for i := 0; i < n; i++ {
		w := list[i]
		start, _ := time.Parse(time.RFC3339, w.Start)
		dateStr := start.In(ist).Format("Mon, 2 Jan 2006 15:04 MST")

		desc := w.Summary
		if desc == "" {
			desc = w.Title
		}
		// Build a richer description
		details := []string{}
		if w.Region != "" {
			details = append(details, "Region: "+w.Region)
		}
		if w.Dur != "" {
			details = append(details, "Duration: "+w.Dur)
		}
		if w.Platform != "" {
			details = append(details, "Platform: "+w.Platform)
		}
		if w.Speakers != "" {
			details = append(details, "Speakers: "+w.Speakers)
		}
		if len(details) > 0 {
			desc = desc + "\n\n" + strings.Join(details, " | ")
		}

		b.WriteString("  <item>\n")
		b.WriteString("    <title>" + xmlEsc(w.Title) + " — " + dateStr + "</title>\n")
		b.WriteString("    <link>" + xmlEsc(w.URL) + "</link>\n")
		b.WriteString("    <guid isPermaLink=\"false\">" + xmlEsc(w.ID) + "</guid>\n")
		b.WriteString("    <pubDate>" + start.UTC().Format(time.RFC1123Z) + "</pubDate>\n")
		b.WriteString("    <description>" + xmlEsc(desc) + "</description>\n")
		if w.Region != "" {
			b.WriteString("    <category>" + xmlEsc(w.Region) + "</category>\n")
		}
		b.WriteString("  </item>\n")
	}

	b.WriteString("</channel>\n</rss>\n")
	return b.String()
}

// xmlEsc escapes special XML characters.
func xmlEsc(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	s = strings.ReplaceAll(s, "'", "&#39;")
	return s
}

func clip(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return strings.TrimSpace(string(r[:n])) + "…"
}

// ---------------------------------------------------------------- server

const version = "1.0.0"

const helpText = `awsworkshops ` + version + ` — upcoming AWS online hands-on workshops, in IST

  Harvests the AWS Experience event API across 11 regional portals, keeps only
  sessions that are BOTH online AND hands-on AND still upcoming, converts every
  time to Asia/Kolkata, and serves a filterable page. Self-contained: no config,
  no data files, no network access beyond aws-experience.com.

USAGE
  awsworkshops [flags]

EXAMPLES
  awsworkshops                       start on http://127.0.0.1:8765
  awsworkshops -open                 start and open your browser
  awsworkshops -port 9000            use another port
  awsworkshops -refresh 1h           re-harvest hourly instead of every 6h
  awsworkshops -refresh 0            harvest once, never again
  awsworkshops -dump out.json        write JSON and exit, no server
  awsworkshops -static _site/index.html
                                     write a self-contained page and exit
  awsworkshops -static _site/index.html -rss _site/feed.xml
                                     write the page and RSS feed together
  awsworkshops -addr 0.0.0.0         expose on the network (SEE SECURITY)

FLAGS
  -addr string      interface to bind (default "127.0.0.1")
  -port int         port to listen on (default 8765)
  -refresh dur      background re-harvest interval, 0 disables (default 6h)
  -open             open the page in your browser once ready
  -dump file        write harvested JSON to file and exit
  -static file      write the rendered page to file and exit. Server-only
                    controls (Refresh, JSON, /healthz, /refresh) are omitted
  -rss file         write RSS feed to file (use with -static for GitHub Pages)
  -version          print version and exit
  -h, -help         show this help

ROUTES
  /                 the page
  /api/workshops    the same data as JSON
  /refresh          re-harvest immediately
  /healthz          workshop count, build time, per-endpoint warnings

SECURITY
  There is no authentication. The default bind of 127.0.0.1 means only this
  machine can reach it. Binding anywhere else lets anyone who can reach this
  host read the page, so only do that on a network you trust.

NOTES
  Startup takes ~10s: 22 requests are paced deliberately because the origin
  throttles bursts. A failed background refresh keeps the previous data rather
  than emptying the page — check /healthz to see which endpoints failed.
`

func main() {
	addr := flag.String("addr", "127.0.0.1", "interface to bind (127.0.0.1 keeps it local-only)")
	port := flag.Int("port", 8765, "port to listen on")
	every := flag.Duration("refresh", 6*time.Hour, "how often to re-harvest in the background (0 disables)")
	open := flag.Bool("open", false, "open the page in your browser once ready")
	dump := flag.String("dump", "", "write the harvested JSON to this file and exit (no server)")
	static := flag.String("static", "", "write the fully rendered HTML page to this file and exit (for GitHub Pages)")
	rss := flag.String("rss", "", "write an RSS feed to this file (use with -static for GitHub Pages)")
	showVer := flag.Bool("version", false, "print version and exit")

	flag.Usage = func() { fmt.Fprint(os.Stderr, helpText) }
	flag.Parse()

	if *showVer {
		fmt.Println("awsworkshops " + version)
		return
	}

	var err error
	if ist, err = time.LoadLocation("Asia/Kolkata"); err != nil {
		log.Fatalf("timezone: %v", err) // cannot happen: tzdata is embedded
	}

	st := &store{}

	log.Printf("harvesting %d portals x %d endpoints…", len(portals), len(endpoints))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	ws, warn := harvest(ctx)
	cancel()
	st.set(ws, warn)
	report(ws, warn)

	if *dump != "" {
		b, _ := json.MarshalIndent(ws, "", "  ")
		if err := os.WriteFile(*dump, b, 0o644); err != nil {
			log.Fatalf("dump: %v", err)
		}
		log.Printf("wrote %s (%d workshops)", *dump, len(ws))
		return
	}

	if *static != "" {
		page := renderPage(ws, time.Now(), true)
		if err := os.WriteFile(*static, []byte(page), 0o644); err != nil {
			log.Fatalf("static: %v", err)
		}
		log.Printf("wrote %s (%d workshops, %d bytes)", *static, len(ws), len(page))

		if *rss != "" {
			feed := renderRSS(ws, time.Now())
			if err := os.WriteFile(*rss, []byte(feed), 0o644); err != nil {
				log.Fatalf("rss: %v", err)
			}
			log.Printf("wrote %s (%d items)", *rss, min(len(ws), 50))
		}
		return
	}

	if len(ws) == 0 {
		log.Printf("WARNING: no workshops harvested — serving an empty page")
	}

	if *every > 0 {
		go func() {
			for range time.Tick(*every) {
				c, cn := context.WithTimeout(context.Background(), 5*time.Minute)
				n, w := harvest(c)
				cn()
				if len(n) > 0 {
					st.set(n, w)
					log.Printf("refreshed: %d workshops", len(n))
				} else {
					log.Printf("refresh failed, keeping previous %d", len(ws))
				}
			}
		}()
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/{$}", func(w http.ResponseWriter, r *http.Request) {
		list, built, _ := st.get()
		page := renderPage(list, built, false)
		w.Header().Set("content-type", "text/html; charset=utf-8")
		w.Header().Set("cache-control", "no-store")
		io.WriteString(w, page)
	})

	mux.HandleFunc("/api/workshops", func(w http.ResponseWriter, r *http.Request) {
		list, _, _ := st.get()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(list)
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		list, built, warn := st.get()
		w.Header().Set("content-type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"workshops": len(list),
			"built":     built.In(ist).Format(time.RFC3339),
			"warnings":  warn,
		})
	})

	mux.HandleFunc("/refresh", func(w http.ResponseWriter, r *http.Request) {
		c, cn := context.WithTimeout(r.Context(), 5*time.Minute)
		defer cn()
		n, warn := harvest(c)
		if len(n) == 0 {
			http.Error(w, "harvest returned nothing; previous data kept", http.StatusBadGateway)
			return
		}
		st.set(n, warn)
		fmt.Fprintf(w, "refreshed: %d workshops\n", len(n))
	})

	listen := fmt.Sprintf("%s:%d", *addr, *port)
	srv := &http.Server{
		Addr:              listen,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	url := fmt.Sprintf("http://%s/", listen)
	log.Printf("serving %s", url)
	if *addr != "127.0.0.1" && *addr != "localhost" {
		log.Printf("NOTE: bound to %s — this has no authentication, so anyone who can reach "+
			"this host can read the page. Use 127.0.0.1 unless you intend to share it.", *addr)
	}
	if *open {
		go browse(url)
	}

	// shut down cleanly on Ctrl-C
	idle := make(chan struct{})
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		log.Printf("shutting down…")
		c, cn := context.WithTimeout(context.Background(), 5*time.Second)
		defer cn()
		srv.Shutdown(c)
		close(idle)
	}()

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("listen: %v", err)
	}
	<-idle
}

func report(ws []Workshop, warn []string) {
	byRegion := map[string]int{}
	days := map[string]bool{}
	for _, w := range ws {
		byRegion[w.Region]++
		// Day counts are reported in IST purely so the operator's log matches the
		// page's default view; the page itself derives dates in the reader's zone.
		if t, err := time.Parse(time.RFC3339, w.Start); err == nil {
			days[t.In(ist).Format("2006-01-02")] = true
		}
	}
	parts := make([]string, 0, len(byRegion))
	for _, r := range []string{"AMER", "EMEA", "APJ", "LATAM"} {
		if n := byRegion[r]; n > 0 {
			parts = append(parts, fmt.Sprintf("%s %d", r, n))
		}
	}
	log.Printf("%d online hands-on workshops across %d days (%s)",
		len(ws), len(days), strings.Join(parts, ", "))
	for _, w := range warn {
		log.Printf("  warn: %s", w)
	}
}

func browse(url string) {
	time.Sleep(300 * time.Millisecond)
	var cmd string
	var args []string
	switch runtime.GOOS {
	case "darwin":
		cmd = "open"
	case "windows":
		cmd, args = "rundll32", []string{"url.dll,FileProtocolHandler"}
	default:
		cmd = "xdg-open"
	}
	exec.Command(cmd, append(args, url)...).Start()
}

// ---------------------------------------------------------------- embedded UI
//
// One raw string literal: no asset files, no templates on disk. Note that Go raw
// strings cannot contain a backtick, so the JS below uses string concatenation
// rather than template literals.

const pageHTML = `<!doctype html>
<html lang="en" data-theme="auto">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Online Hands-on Workshops</title>
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='4' fill='%23f5efe4'/%3E%3Ctext x='16' y='23' font-family='Georgia,serif' font-size='20' font-weight='600' text-anchor='middle' fill='%23a03c1e'%3EW%3C/text%3E%3C/svg%3E">
<link rel="alternate" type="application/rss+xml" title="AWS Online Hands-on Workshops" href="feed.xml">
<script>
(function(){var t=null;try{t=localStorage.getItem('reg-theme');}catch(e){}
var r=document.documentElement;r.setAttribute('data-theme',t||'auto');
try{if(window.matchMedia('(prefers-color-scheme: dark)').matches)r.classList.add('sys-dark');}catch(e){}})();
</script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Newsreader:wght@300;500;600&family=IBM+Plex+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<style>
/* Hallmark · pre-emit critique: P5 H5 E5 S5 R4 V5 */
/* Hallmark · macrostructure: 13 · Index-First */
/* Hallmark · theme: Almanac */
:root{
  --font-display:'Newsreader',Georgia,'Times New Roman',serif;
  --font-body:'IBM Plex Sans',ui-sans-serif,'Segoe UI',sans-serif;
  --font-mono:'JetBrains Mono',ui-monospace,'Courier New',monospace;

  --l-paper:oklch(97% 0.011 84);   --l-paper-2:oklch(94.2% 0.015 84);
  --l-paper-3:oklch(91% 0.018 84); --l-rule:oklch(85% 0.016 84);
  --l-rule-strong:oklch(72% 0.02 84); --l-ink:oklch(21% 0.012 60);
  --l-ink-2:oklch(42% 0.014 70);   --l-ink-3:oklch(50% 0.013 70);
  --l-accent:oklch(47% 0.17 38);   --l-accent-2:oklch(41% 0.15 38);
  --l-accent-soft:oklch(93% 0.035 45); --l-live:oklch(43% 0.12 152);

  /* Dark is not a mirror of light. The base sits at L22 (~rgb 29,26,23) — a warm
     charcoal in the same register as VS Code's dark, rather than the near-black
     it used to be — and the accent drops from neon orange to a clay terracotta
     that still belongs to the light theme's rust family. */
  --d-paper:oklch(22% 0.008 65);    --d-paper-2:oklch(26% 0.010 65);
  --d-paper-3:oklch(30% 0.012 65);  --d-rule:oklch(36% 0.012 65);
  --d-rule-strong:oklch(48% 0.014 65); --d-ink:oklch(93% 0.010 80);
  --d-ink-2:oklch(78% 0.012 78);    --d-ink-3:oklch(69% 0.013 76);
  --d-accent:oklch(70% 0.10 42);    --d-accent-2:oklch(79% 0.09 46);
  --d-accent-soft:oklch(30% 0.035 42); --d-live:oklch(74% 0.09 158);

  --paper:var(--l-paper); --paper-2:var(--l-paper-2); --paper-3:var(--l-paper-3);
  --rule:var(--l-rule); --rule-strong:var(--l-rule-strong);
  --ink:var(--l-ink); --ink-2:var(--l-ink-2); --ink-3:var(--l-ink-3);
  --accent:var(--l-accent); --accent-2:var(--l-accent-2);
  --accent-soft:var(--l-accent-soft); --live:var(--l-live);

  --text-xs:0.72rem; --text-sm:0.82rem; --text-base:0.95rem;
  --text-md:1.15rem; --text-lg:1.5rem; --text-xl:2rem; --text-2xl:2.6rem;
  --space-2xs:0.25rem; --space-xs:0.5rem; --space-sm:0.75rem; --space-md:1rem;
  --space-lg:1.5rem; --space-xl:2.5rem; --space-2xl:4rem;
  --z-dropdown:100; --z-sticky:200;
  color-scheme:light;
}
:root[data-theme="dark"], :root[data-theme="auto"].sys-dark{
  --paper:var(--d-paper); --paper-2:var(--d-paper-2); --paper-3:var(--d-paper-3);
  --rule:var(--d-rule); --rule-strong:var(--d-rule-strong);
  --ink:var(--d-ink); --ink-2:var(--d-ink-2); --ink-3:var(--d-ink-3);
  --accent:var(--d-accent); --accent-2:var(--d-accent-2);
  --accent-soft:var(--d-accent-soft); --live:var(--d-live);
  color-scheme:dark;
}
*{box-sizing:border-box}
html{overflow-x:clip;-webkit-text-size-adjust:100%}
body{overflow-x:clip;margin:0;background:var(--paper);color:var(--ink);
  font-family:var(--font-body);font-size:var(--text-base);line-height:1.5;
  -webkit-font-smoothing:antialiased}
a{color:var(--accent);text-decoration:none}
a:hover{text-decoration:underline;text-underline-offset:2px}
a:focus-visible,button:focus-visible,input:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
.wrap{max-width:1160px;margin:0 auto;padding:0 var(--space-md)}

.masthead{border-bottom:2px solid var(--ink);padding:var(--space-md) 0 var(--space-sm)}
.mastline{display:flex;gap:var(--space-xs) var(--space-md);align-items:center;
  justify-content:space-between;flex-wrap:wrap}
.kicker{font-family:var(--font-mono);font-size:var(--text-xs);letter-spacing:.14em;
  text-transform:uppercase;color:var(--ink-3);margin:0 0 var(--space-xs)}
h1{font-family:var(--font-display);font-weight:600;font-size:clamp(1.75rem,3.6vw + .5rem,var(--text-xl));
  line-height:1.05;margin:var(--space-2xs) 0 var(--space-xs);letter-spacing:-.015em}
.standfirst{margin:0;color:var(--ink-2);max-width:70ch;font-size:var(--text-sm)}
.standfirst strong{color:var(--ink);font-weight:600}
.tzbadge{display:inline-block;font-family:var(--font-mono);font-size:var(--text-xs);
  background:var(--accent-soft);color:var(--accent-2);border:1px solid var(--accent);
  padding:1px var(--space-xs);border-radius:2px;white-space:nowrap}
.mastutil{display:flex;flex-wrap:wrap;align-items:center;gap:var(--space-xs);
  min-width:0;margin-left:auto}
.upd{font-family:var(--font-mono);font-size:var(--text-xs);color:var(--ink-3);white-space:nowrap}
.mastlink{font-family:var(--font-mono);font-size:var(--text-xs);color:var(--ink-3);
  text-decoration:underline;text-underline-offset:2px}
.mastlink:hover{color:var(--accent)}
#refresh[aria-busy=true]{opacity:.6;cursor:progress}
.themeswitch{display:flex;flex-shrink:0}
.themeswitch button{font-family:var(--font-mono);font-size:var(--text-xs);letter-spacing:.06em;
  text-transform:uppercase;background:var(--paper);color:var(--ink-3);
  border:1px solid var(--rule-strong);padding:3px var(--space-xs);cursor:pointer;
  transition:background 120ms ease,color 120ms ease}
.themeswitch button+button{border-left:0}
.themeswitch button:first-child{border-radius:2px 0 0 2px}
.themeswitch button:last-child{border-radius:0 2px 2px 0}
.themeswitch button:hover{color:var(--ink)}
.themeswitch button[aria-pressed=true]{background:var(--ink);color:var(--paper);border-color:var(--ink)}

.filters{position:sticky;top:0;z-index:var(--z-sticky);background:var(--paper-2);
  border-bottom:1px solid var(--rule-strong);padding:var(--space-sm) 0}
.frow{display:flex;flex-wrap:wrap;gap:var(--space-sm);align-items:flex-end}
.field{display:flex;flex-direction:column;gap:2px;min-width:0}
.field label{font-family:var(--font-mono);font-size:var(--text-xs);letter-spacing:.08em;
  text-transform:uppercase;color:var(--ink-3)}
.field.grow{flex:1 1 300px;max-width:440px}
.field.grow input{width:100%}
input[type=search]{font-family:var(--font-body);font-size:var(--text-sm);color:var(--ink);
  background:var(--paper);border:1px solid var(--rule-strong);border-radius:2px;
  padding:var(--space-2xs) var(--space-xs);min-height:32px}
.kbd{font-family:var(--font-mono);font-size:9px;border:1px solid var(--rule-strong);
  border-radius:2px;padding:0 3px;color:var(--ink-3);margin-left:2px}
.presets{display:flex;flex-wrap:wrap;gap:var(--space-2xs)}
.chip{font-family:var(--font-mono);font-size:var(--text-xs);letter-spacing:.04em;
  background:var(--paper);color:var(--ink-2);border:1px solid var(--rule-strong);
  border-radius:2px;padding:var(--space-2xs) var(--space-xs);cursor:pointer;
  transition:background 120ms ease,color 120ms ease,border-color 120ms ease}
.chip:hover{border-color:var(--ink-3);color:var(--ink)}
.chip:active{background:var(--paper-3)}
.chip[aria-pressed=true]{background:var(--ink);color:var(--paper);border-color:var(--ink)}
.rangenow{font-family:var(--font-mono);font-size:var(--text-sm);color:var(--ink);
  margin:0;padding:4px 0;white-space:nowrap}

.calbar{display:grid;grid-template-columns:300px minmax(0,1fr);gap:var(--space-xl);
  align-items:start;padding:var(--space-sm) 0 var(--space-md);border-bottom:1px solid var(--rule)}
.cal{width:100%}
.calhead .m{font-size:var(--text-base)}
.calbar.collapsed{grid-template-columns:minmax(0,1fr);padding-top:var(--space-md)}
.calbar.collapsed .cal{display:none}
.calhead{display:flex;align-items:center;gap:var(--space-2xs);margin-bottom:var(--space-xs)}
.calhead .m{font-family:var(--font-display);font-size:var(--text-md);font-weight:600;flex:1}
.calnav{font-family:var(--font-mono);font-size:var(--text-base);line-height:1;
  background:var(--paper);border:1px solid var(--rule-strong);color:var(--ink-2);
  border-radius:2px;width:26px;height:26px;cursor:pointer;
  transition:color 120ms ease,border-color 120ms ease}
.calnav:hover:not(:disabled){color:var(--ink);border-color:var(--ink-3)}
.calnav:disabled{opacity:.35;cursor:not-allowed}
.calgrid{display:grid;grid-template-columns:repeat(7,minmax(0,1fr));gap:2px}
.cal-wd{font-family:var(--font-mono);font-size:var(--text-xs);color:var(--ink-3);
  text-align:center;padding-bottom:3px}
.cal-d{font-family:var(--font-mono);font-size:var(--text-sm);text-align:center;
  padding:2px 0;border:1px solid transparent;border-radius:2px;
  color:var(--ink-3);background:none;min-height:28px;line-height:24px}
.cal-d.pad{visibility:hidden}
button.cal-d{cursor:pointer;color:var(--ink);border-color:var(--rule-strong);
  background:var(--paper);font-weight:500;
  transition:background 120ms ease,color 120ms ease,border-color 120ms ease}
button.cal-d:hover{border-color:var(--ink)}
button.cal-d.d1{background:var(--paper-2)}
button.cal-d.d2{background:var(--paper-3)}
button.cal-d.d3{background:var(--rule)}
button.cal-d[aria-pressed=true]{background:var(--ink);color:var(--paper);border-color:var(--ink)}
.cal-d.today{border-color:var(--accent);color:var(--accent)}

.tally{display:flex;flex-direction:column;align-items:stretch;gap:var(--space-md)}
.tallynums{display:flex;gap:var(--space-lg);align-items:baseline}
.tally .n{font-family:var(--font-display);font-size:var(--text-xl);font-weight:600;line-height:1}
.tally .lbl{font-family:var(--font-mono);font-size:var(--text-xs);text-transform:uppercase;
  letter-spacing:.1em;color:var(--ink-3)}
.glance{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));
  gap:var(--space-lg) var(--space-xl);width:100%;max-width:620px;margin:0}
@media (max-width:760px){.glance{grid-template-columns:minmax(0,1fr);max-width:none}}
.glance dt{font-family:var(--font-mono);font-size:var(--text-xs);letter-spacing:.1em;
  text-transform:uppercase;color:var(--ink-3);margin-bottom:3px}
.glance dd{margin:0;font-size:var(--text-sm);color:var(--ink);line-height:1.4}
.glance .sub{color:var(--ink-3);font-family:var(--font-mono);font-size:var(--text-xs);display:block}

.day{margin-bottom:var(--space-lg)}
.dayhead{display:flex;gap:var(--space-sm);align-items:baseline;flex-wrap:wrap;
  padding:var(--space-md) 0 var(--space-xs);position:sticky;top:var(--filterh,76px);
  z-index:var(--z-dropdown);background:var(--paper);border-bottom:1px solid var(--rule-strong)}
.dayhead .dnum{font-family:var(--font-display);font-size:var(--text-lg);font-weight:600;line-height:1}
.dayhead .dwk{font-family:var(--font-mono);font-size:var(--text-xs);text-transform:uppercase;
  letter-spacing:.12em;color:var(--ink-3)}
.dayhead .dcount{margin-left:auto;font-family:var(--font-mono);font-size:var(--text-xs);color:var(--ink-3)}
.dayhead.is-today .dnum{color:var(--accent)}
.today-tag{font-family:var(--font-mono);font-size:var(--text-xs);background:var(--accent);
  color:var(--paper);padding:1px 6px;border-radius:2px;letter-spacing:.08em}

.ev{display:grid;grid-template-columns:8.5rem minmax(0,1fr);gap:var(--space-md);
  padding:var(--space-sm) var(--space-xs);border-top:1px dotted var(--rule);
  align-items:start;margin:0 calc(var(--space-xs) * -1);border-radius:2px;
  transition:background 100ms ease}
.ev:hover{background:var(--paper-2)}
.day .ev:first-of-type{border-top:0}
.rail{font-family:var(--font-mono);font-size:var(--text-sm);line-height:1.35}
.rail .t{font-weight:700;color:var(--ink)}
.rail .te{color:var(--ink-2)}
.rail .dur{color:var(--ink-3);font-size:var(--text-xs);display:block}
.evtitle{font-family:var(--font-display);font-size:var(--text-md);font-weight:600;
  line-height:1.2;margin:0 0 var(--space-2xs)}
.evtitle a{color:var(--ink)}
.evtitle a:hover{color:var(--accent)}
.meta{display:flex;flex-wrap:wrap;gap:var(--space-2xs) var(--space-xs);
  font-family:var(--font-mono);font-size:var(--text-xs);color:var(--ink-3);
  align-items:center;margin-bottom:var(--space-2xs)}
.tag{border:1px solid var(--rule-strong);border-radius:2px;padding:0 5px;white-space:nowrap}
.tag.live-now{background:var(--live);color:var(--paper);border-color:var(--live)}
.orig{color:var(--ink-3);font-family:var(--font-mono);font-size:var(--text-xs)}
.summary{margin:var(--space-2xs) 0 0;color:var(--ink-2);font-size:var(--text-sm);max-width:78ch}
.go{display:inline-flex;align-items:center;gap:5px;margin-top:var(--space-xs);
  font-family:var(--font-mono);font-size:var(--text-xs);letter-spacing:.06em;
  text-transform:uppercase;text-decoration:none;background:transparent;color:var(--accent);
  border:1px solid var(--accent);border-radius:2px;padding:4px var(--space-sm);
  transition:background 120ms ease,color 120ms ease}
.go:hover{background:var(--accent);color:var(--paper);text-decoration:none}
.go.alt{color:var(--ink-3);border-color:var(--rule-strong)}
.go.alt:hover{background:transparent;color:var(--ink);border-color:var(--ink-3)}

.empty{padding:var(--space-2xl) 0;color:var(--ink-2);max-width:60ch}
.empty h2{font-family:var(--font-display);font-weight:600;font-size:var(--text-lg);margin:0 0 var(--space-xs)}
footer.foot{border-top:2px solid var(--ink);margin-top:var(--space-xl);
  padding:var(--space-lg) 0 var(--space-2xl);color:var(--ink-2);font-size:var(--text-sm)}
.footgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(260px,100%),1fr));
  gap:var(--space-lg) var(--space-xl)}
.footgrid p{margin:0;max-width:46ch;line-height:1.5}
.footgrid strong{color:var(--ink);font-weight:600}
.foothd{font-family:var(--font-mono);font-size:var(--text-xs);letter-spacing:.1em;
  text-transform:uppercase;color:var(--ink-3);margin:0 0 var(--space-xs);font-weight:400}
.footlist{margin:0;padding:0;list-style:none}
.footlist li{margin-bottom:3px}
.footlist a{font-family:var(--font-mono);font-size:var(--text-xs)}
.footnote{margin:var(--space-lg) 0 0;padding-top:var(--space-sm);
  border-top:1px solid var(--rule);color:var(--ink-3);font-size:var(--text-xs);max-width:70ch}

@media (max-width:760px){.calbar{grid-template-columns:minmax(0,1fr);gap:var(--space-md)}}
@media (max-width:640px){
  .ev{grid-template-columns:minmax(0,1fr);gap:var(--space-2xs)}
  .rail{display:flex;gap:var(--space-xs);align-items:baseline}
  .rail .dur{display:inline}
  /* a 190px sticky filter bar eats a phone screen — scroll it away instead */
  .filters{position:static}
  .dayhead{top:0}
  .field.grow{flex:1 1 100%;max-width:none}
}
@media (prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
</style>
</head>
<body>

<header class="masthead">
  <div class="wrap">
    <div class="mastline">
      <p class="kicker">aws-experience.com · 11 portals · 4 regions</p>
      <div class="mastutil">
        <span class="upd" id="updated" data-built="__BUILT_ISO__">updated __BUILT__</span>
        __REFRESHBTN__
        __JSONLINK__
        <a class="mastlink" href="feed.xml" target="_blank" rel="noopener" title="RSS Feed">
          <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="vertical-align:-2px;"><path d="M4 11a9 9 0 0 1 9 9"/><path d="M4 4a16 16 0 0 1 16 16"/><circle cx="5" cy="19" r="1"/></svg> RSS</a>
        <span style="color:var(--rule-strong);margin:0 4px;">|</span>
        <a class="mastlink" href="https://github.com/Paresh-Maheshwari/aws-workshops" target="_blank" rel="noopener" title="Star on GitHub">
          <svg xmlns="http://www.w3.org/2000/svg" width="15" height="15" viewBox="0 0 24 24" fill="currentColor" style="vertical-align:-3px;margin-right:2px;"><path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"/></svg> GitHub</a>
        <div class="themeswitch" role="group" aria-label="Colour theme">
          <button type="button" data-theme-set="auto" aria-pressed="true">Auto</button>
          <button type="button" data-theme-set="light" aria-pressed="false">Light</button>
          <button type="button" data-theme-set="dark" aria-pressed="false">Dark</button>
        </div>
      </div>
    </div>
    <h1>Online Hands-on Workshops</h1>
    <p class="standfirst"><strong>__COUNT__ workshops</strong> &mdash; every AWS Experience session
      that is virtual, hands-on and still to come, from 11 portals across four regions.
      Times shown in <span class="tzbadge" id="tzbadge">IST · Asia/Kolkata</span>.</p>
  </div>
</header>

<section class="filters" aria-label="Filter workshops">
  <div class="wrap">
    <div class="frow">
      <div class="field grow">
        <label for="q">Search <kbd class="kbd">/</kbd></label>
        <input type="search" id="q" placeholder="title, speaker, service — e.g. bedrock agent" autocomplete="off">
      </div>
      <div class="field">
        <label>Range</label>
        <div class="presets">
          <button class="chip" type="button" data-preset="today">Today</button>
          <button class="chip" type="button" data-preset="7">Next 7 days</button>
          <button class="chip" type="button" data-preset="30">Next 30 days</button>
          <button class="chip" type="button" data-preset="all" aria-pressed="true">All</button>
        </div>
      </div>
      <div class="field">
        <label>Showing</label>
        <p class="rangenow" id="rangenow">everything upcoming</p>
      </div>
      <div class="field">
        <label>View</label>
        <div class="presets">
          <button class="chip" type="button" id="calToggle" aria-expanded="true"
            aria-controls="calwrap">Hide calendar</button>
        </div>
      </div>
    </div>
  </div>
</section>

<main class="wrap">
  <div class="calbar">
    <div class="cal" id="calwrap">
      <div class="calhead">
        <span class="m" id="calmon">&mdash;</span>
        <button class="calnav" type="button" id="calprev" aria-label="Previous month">&lsaquo;</button>
        <button class="calnav" type="button" id="calnext" aria-label="Next month">&rsaquo;</button>
      </div>
      <div class="calgrid" id="calgrid" role="grid" aria-label="Workshop dates"></div>
    </div>
    <div class="tally" id="tally" aria-live="polite"></div>
  </div>
  <div id="list"></div>
</main>

<footer class="foot">
  <div class="wrap">
    <div class="footgrid">
      <section>
        <h2 class="foothd">What is included</h2>
        <p>An event appears here only if it is <strong>virtual</strong>, a <strong>hands-on
          workshop</strong>, marked live, and has not started yet. Popular workshops recur: the
          same title runs on many dates with different hosts, and each run is its own event with
          its own page &mdash; so every run is listed separately, never merged.</p>
      </section>
      <section>
        <h2 class="foothd">Worth knowing</h2>
        <p>Seat counts are the organiser's stated capacity, not live availability &mdash; check the
          event page. A handful of sessions have no dedicated page upstream and link to their
          portal calendar instead, labelled as such.</p>
      </section>
      __FOOTSERVER__
    </div>
    <p class="footnote">Event data belongs to Amazon Web Services and comes from
      aws-experience.com. This is an unofficial view, not affiliated with AWS.</p>
  </div>
</footer>

<script>
var W = __DATA__;

var MONTHNAMES = ['January','February','March','April','May','June',
                  'July','August','September','October','November','December'];
var MONTHABBR = ['Jan','Feb','Mar','Apr','May','Jun',
                 'Jul','Aug','Sep','Oct','Nov','Dec'];

var $ = function(id){ return document.getElementById(id); };
var list = $('list'), tally = $('tally'), q = $('q');

function esc(s){
  return String(s == null ? '' : s).replace(/[&<>"']/g, function(c){
    return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];
  });
}
function pad(n){ return (n < 10 ? '0' : '') + n; }
function keyOf(d){ return d.getUTCFullYear() + '-' + pad(d.getUTCMonth()+1) + '-' + pad(d.getUTCDate()); }

// ---- timezone -------------------------------------------------------------
// Every date and clock time on this page is derived here, from the UTC instants
// in the data. Switching zone therefore re-groups the whole listing — day
// headings, calendar and counts included — with no request to any origin.
// IST is only a fallback, for an engine that reports nothing usable.
var FALLBACK_TZ = 'Asia/Kolkata';

// The zone comes from the reader's own machine, through the browser's Intl API:
//
//   Intl.DateTimeFormat().resolvedOptions().timeZone   ->  e.g. "Asia/Calcutta"
//
// That value is read from the OS clock settings. There is no request, no IP
// geolocation and nothing for the reader to choose. Because the zone is applied
// per instant rather than as a fixed offset, DST resolves correctly for events
// on either side of a transition.
var TZ = (function(){
  try {
    var z = Intl.DateTimeFormat().resolvedOptions().timeZone;
    if (!z) return FALLBACK_TZ;
    // prove the zone actually formats before committing to it
    new Intl.DateTimeFormat('en-GB', { timeZone:z }).format(new Date());
    return z;
  } catch (e){ return FALLBACK_TZ; }
})();

// A readable name for the zone. shortGeneric yields "India Time" or "PT" where
// short would give a bare "GMT+5:30"; the id is the fallback.
function zoneLabel(zone){
  try {
    var ps = new Intl.DateTimeFormat(undefined, { timeZone:zone, timeZoneName:'shortGeneric' })
               .formatToParts(new Date());
    for (var i = 0; i < ps.length; i++){
      if (ps[i].type === 'timeZoneName' && ps[i].value) return ps[i].value;
    }
  } catch (e){}
  return zone;
}

// One formatter per zone, built once. formatToParts is used rather than format
// so the pieces can be reassembled without parsing a locale-specific string.
var FMT = {};
function partsIn(d, zone){
  var f = FMT[zone];
  if (!f){
    f = new Intl.DateTimeFormat('en-GB', {
      timeZone: zone, hourCycle: 'h23',
      year:'numeric', month:'2-digit', day:'2-digit',
      hour:'2-digit', minute:'2-digit', weekday:'long'
    });
    FMT[zone] = f;
  }
  var o = {}, ps = f.formatToParts(d);
  for (var i = 0; i < ps.length; i++) o[ps[i].type] = ps[i].value;
  return o;
}

// COUNTS, MONTHKEYS and todayKey all depend on the selected zone, so they are
// rebuilt whenever it changes rather than computed once at load.
var COUNTS, MONTHKEYS, MONTHLABEL, todayKey;

function deriveAll(){
  COUNTS = {}; MONTHKEYS = []; MONTHLABEL = {};

  for (var i = 0; i < W.length; i++){
    var e = W[i];
    if (!e._d){
      e._d = new Date(e.start);
      e._end = e.end ? new Date(e.end) : null;
      e._hay = [e.title, e.speakers, e.series, e.summary, e.platform, e.region, e.language]
                 .join(' ').toLowerCase();
    }
    var o = partsIn(e._d, TZ);
    e.day = o.year + '-' + o.month + '-' + o.day;
    e.time = o.hour + ':' + o.minute;
    e.weekday = o.weekday;
    e.dayLabel = (+o.day) + ' ' + MONTHNAMES[+o.month - 1] + ' ' + o.year;
    if (e._end && !isNaN(e._end.getTime())){
      var oe = partsIn(e._end, TZ);
      e.endTime = oe.hour + ':' + oe.minute;
    } else {
      e.endTime = '';
    }

    COUNTS[e.day] = (COUNTS[e.day] || 0) + 1;
    var mk = e.day.slice(0,7);
    if (MONTHKEYS.indexOf(mk) < 0){
      MONTHKEYS.push(mk);
      MONTHLABEL[mk] = MONTHNAMES[+mk.slice(5,7) - 1] + ' ' + mk.slice(0,4);
    }
  }
  MONTHKEYS.sort();

  // "today" in the selected zone, never in the viewer's own clock zone
  var n = partsIn(new Date(), TZ);
  todayKey = n.year + '-' + n.month + '-' + n.day;
}
deriveAll();

var RANGE = { from:'', to:'' };

function shiftKey(n){
  var p = todayKey.split('-');
  var d = new Date(Date.UTC(+p[0], +p[1]-1, +p[2]));
  d.setUTCDate(d.getUTCDate() + n);
  return keyOf(d);
}
function labelOf(key){
  var p = key.split('-');
  return (+p[2]) + ' ' + MONTHNAMES[+p[1]-1] + ' ' + p[0];
}
function abbrOf(key){
  var p = key.split('-');
  return (+p[2]) + ' ' + MONTHABBR[+p[1]-1];
}

// ---- calendar -------------------------------------------------------------
var calCursor = MONTHKEYS.indexOf(todayKey.slice(0,7)) >= 0 ? todayKey.slice(0,7)
              : (MONTHKEYS[0] || todayKey.slice(0,7));
var WD = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];

function renderCal(){
  var y = +calCursor.slice(0,4), m = +calCursor.slice(5,7);
  $('calmon').textContent = MONTHLABEL[calCursor] || (MONTHNAMES[m-1] + ' ' + y);
  var idx = MONTHKEYS.indexOf(calCursor);
  $('calprev').disabled = idx <= 0;
  $('calnext').disabled = idx < 0 || idx >= MONTHKEYS.length - 1;

  var lead = (new Date(Date.UTC(y, m-1, 1)).getUTCDay() + 6) % 7;
  var days = new Date(Date.UTC(y, m, 0)).getUTCDate();
  var cells = [];
  for (var w = 0; w < 7; w++) cells.push('<div class="cal-wd">' + WD[w] + '</div>');
  for (var p = 0; p < lead; p++) cells.push('<div class="cal-d pad"></div>');

  for (var d = 1; d <= days; d++){
    var key = calCursor + '-' + pad(d);
    var n = COUNTS[key] || 0;
    var inRange = RANGE.from && RANGE.to && key >= RANGE.from && key <= RANGE.to;
    var cls = 'cal-d' + (key === todayKey ? ' today' : '');
    if (n){
      cls += n >= 8 ? ' d3' : n >= 4 ? ' d2' : ' d1';
      cells.push('<button type="button" class="' + cls + '" data-day="' + key + '"' +
        ' aria-pressed="' + (inRange ? 'true' : 'false') + '"' +
        ' aria-label="' + d + ': ' + n + ' workshop' + (n > 1 ? 's' : '') + '"' +
        ' title="' + n + ' workshop' + (n > 1 ? 's' : '') + '">' + d + '</button>');
    } else {
      cells.push('<div class="' + cls + '">' + d + '</div>');
    }
  }
  $('calgrid').innerHTML = cells.join('');

  var btns = $('calgrid').querySelectorAll('[data-day]');
  for (var b = 0; b < btns.length; b++){
    btns[b].addEventListener('click', function(){
      var key = this.getAttribute('data-day');
      if (RANGE.from && RANGE.from === RANGE.to){
        if (key === RANGE.from){ RANGE.from = ''; RANGE.to = ''; }
        else if (key > RANGE.from){ RANGE.to = key; }
        else { RANGE.from = key; }
      } else {
        RANGE.from = key; RANGE.to = key;
      }
      clearPresets();
      render();
    });
  }
}
function clearPresets(){
  var ps = document.querySelectorAll('[data-preset]');
  for (var i = 0; i < ps.length; i++) ps[i].setAttribute('aria-pressed','false');
}
function gotoMonth(mk){
  calCursor = mk;
  var y = +mk.slice(0,4), m = +mk.slice(5,7);
  RANGE.from = mk + '-01';
  RANGE.to = mk + '-' + pad(new Date(Date.UTC(y, m, 0)).getUTCDate());
  clearPresets();
  render();
}
$('calprev').addEventListener('click', function(){
  var i = MONTHKEYS.indexOf(calCursor); if (i > 0) gotoMonth(MONTHKEYS[i-1]);
});
$('calnext').addEventListener('click', function(){
  var i = MONTHKEYS.indexOf(calCursor); if (i >= 0 && i < MONTHKEYS.length-1) gotoMonth(MONTHKEYS[i+1]);
});

// ---- render ---------------------------------------------------------------
function render(){
  var now = new Date();
  var terms = q.value.trim().toLowerCase().split(/\s+/).filter(Boolean);
  var rows = [];
  for (var i = 0; i < W.length; i++){
    var e = W[i];
    if (RANGE.from && e.day < RANGE.from) continue;
    if (RANGE.to   && e.day > RANGE.to)   continue;
    var ok = true;
    for (var t = 0; t < terms.length; t++){
      if (e._hay.indexOf(terms[t]) < 0){ ok = false; break; }
    }
    if (ok) rows.push(e);
  }

  $('rangenow').textContent =
      !RANGE.from ? 'everything upcoming'
    : RANGE.from === RANGE.to
        ? (RANGE.from === todayKey ? 'today only' : labelOf(RANGE.from))
        : labelOf(RANGE.from) + ' \u2192 ' + labelOf(RANGE.to);

  var order = [], groups = {};
  for (var r = 0; r < rows.length; r++){
    var k = rows[r].day;
    if (!groups[k]){ groups[k] = []; order.push(k); }
    groups[k].push(rows[r]);
  }

  var byRegion = {}, busiest = null;
  for (var g = 0; g < order.length; g++){
    if (!busiest || groups[order[g]].length > groups[busiest].length) busiest = order[g];
  }
  for (var z = 0; z < rows.length; z++) byRegion[rows[z].region] = (byRegion[rows[z].region]||0) + 1;
  var regs = [];
  ['AMER','EMEA','APJ','LATAM'].forEach(function(rg){
    if (byRegion[rg]) regs.push(rg + '\u00a0' + byRegion[rg]);
  });

  var weekEnd = shiftKey(7), week = 0, weekDaySet = {};
  for (var wq = 0; wq < rows.length; wq++){
    if (rows[wq].day >= todayKey && rows[wq].day <= weekEnd){
      week++; weekDaySet[rows[wq].day] = 1;
    }
  }
  var weekDays = Object.keys(weekDaySet).length;

  var nextTxt = '<span class="sub">nothing further ahead</span>';
  for (var n = 0; n < rows.length; n++){
    if (rows[n]._d > now){
      var mins = Math.round((rows[n]._d - now) / 60000);
      var rel = mins < 60 ? 'in ' + mins + ' min'
              : mins < 1440 ? 'in ' + Math.round(mins/60) + 'h'
              : 'in ' + Math.round(mins/1440) + ' days';
      nextTxt = esc(rows[n].title.slice(0,58)) + (rows[n].title.length > 58 ? '\u2026' : '') +
        '<span class="sub">' + esc(rows[n].time) + ', ' + esc(rows[n].dayLabel) +
        '<br>' + rel + '</span>';
      break;
    }
  }

  tally.innerHTML =
    '<div class="tallynums">' +
      '<span><span class="n">' + rows.length + '</span> <span class="lbl">workshops</span></span>' +
      '<span><span class="n">' + order.length + '</span> <span class="lbl">days</span></span>' +
    '</div>' +
    '<dl class="glance">' +
      '<div><dt>Next up</dt><dd>' + nextTxt + '</dd></div>' +
      '<div><dt>By region</dt><dd>' + (regs.join(' · ') || '\u2014') + '</dd></div>' +
      '<div><dt>Busiest day</dt><dd>' + (busiest ? esc(groups[busiest][0].dayLabel) : '\u2014') +
        '<span class="sub">' + (busiest ? groups[busiest].length + ' workshops' : '') + '</span></dd></div>' +
      '<div><dt>Next 7 days</dt><dd>' + week + '<span class="sub">' +
        (weekDays ? 'across ' + weekDays + ' days' : 'nothing scheduled') + '</span></dd></div>' +
    '</dl>';

  if (!rows.length){
    list.innerHTML = '<div class="empty"><h2>Nothing in that range</h2>' +
      '<p>Try a wider date range, or clear the search box.</p></div>';
    syncCal();
    return;
  }

  var out = [];
  for (var o = 0; o < order.length; o++){
    var key = order[o], day = groups[key], isToday = key === todayKey;
    out.push('<section class="day"><div class="dayhead' + (isToday ? ' is-today' : '') + '">' +
      '<span class="dnum">' + esc(day[0].dayLabel) + '</span>' +
      '<span class="dwk">' + esc(day[0].weekday) + '</span>' +
      (isToday ? '<span class="today-tag">TODAY</span>' : '') +
      '<span class="dcount">' + day.length + (day.length > 1 ? ' workshops' : ' workshop') +
      '</span></div>');

    for (var x = 0; x < day.length; x++){
      var e2 = day[x];
      var liveNow = e2._d <= now;
      var bits = [esc(e2.region)];
      if (e2.levels)   bits.push('L' + esc(e2.levels));
      if (e2.language) bits.push(esc(e2.language));
      if (e2.platform) bits.push(esc(e2.platform));
      if (e2.capacity) bits.push(esc(e2.capacity) + ' seats');

      var localLine = '';
      if (e2.localTime){
        // spell the organiser's date out only when it lands on a different day
        // than the zone the reader is viewing in
        var showDate = e2.localDate && e2.localDate !== e2.day;
        localLine = esc(e2.localTime) + ' local' +
          (showDate ? ' ' + esc(abbrOf(e2.localDate)) : '') +
          (e2.tz ? ' \u00b7 ' + esc(e2.tz) : '');
      }

      out.push('<article class="ev"><div class="rail">' +
        '<span class="t">' + esc(e2.time) + '</span>' +
        (e2.endTime ? '<span class="te">\u2013' + esc(e2.endTime) + '</span>' : '') +
        '<span class="dur">' + esc(e2.dur) + '</span></div><div>' +
        '<h3 class="evtitle">' +
          (e2.nopage ? esc(e2.title)
            : '<a href="' + esc(e2.url) + '" target="_blank" rel="noopener">' + esc(e2.title) + '</a>') +
        '</h3>' +
        '<div class="meta">' + (liveNow ? '<span class="tag live-now">LIVE NOW</span>' : '') +
          '<span>' + bits.join(' \u00b7 ') + '</span></div>' +
        '<div class="orig">' + localLine + '</div>' +
        (e2.summary ? '<p class="summary">' + esc(e2.summary) + '</p>' : '') +
        '<a class="go' + (e2.nopage ? ' alt' : '') + '" href="' + esc(e2.url) +
          '" target="_blank" rel="noopener">' +
          (e2.nopage ? 'Find on portal calendar' : 'Open event page') +
          ' <span class="arw">&rarr;</span></a>' +
        '</div></article>');
    }
    out.push('</section>');
  }
  list.innerHTML = out.join('');
  syncCal();
}

function syncCal(){
  if (RANGE.from){
    var mk = RANGE.from.slice(0,7);
    if (RANGE.from === RANGE.to || RANGE.to.slice(0,7) === mk){
      if (MONTHKEYS.indexOf(mk) >= 0) calCursor = mk;
    }
  }
  renderCal();
}

// ---- wiring ---------------------------------------------------------------
q.addEventListener('input', render);

// name the zone the page resolved to, so the reader can see which clock these
// times belong to without having to infer it
(function(){
  var badge = $('tzbadge');
  if (badge) badge.textContent = zoneLabel(TZ) + ' \u00b7 ' + TZ;
})();

// "/" focuses search from anywhere; Esc clears it and returns focus to the page
document.addEventListener('keydown', function(ev){
  var typing = /^(INPUT|TEXTAREA|SELECT)$/.test(document.activeElement.tagName);
  if (ev.key === '/' && !typing){
    ev.preventDefault();
    q.focus();
    q.select();
  } else if (ev.key === 'Escape' && document.activeElement === q){
    if (q.value){ q.value = ''; render(); } else { q.blur(); }
  }
});
var presets = document.querySelectorAll('[data-preset]');
for (var pi = 0; pi < presets.length; pi++){
  presets[pi].addEventListener('click', function(){
    clearPresets();
    this.setAttribute('aria-pressed','true');
    var p = this.getAttribute('data-preset');
    if (p === 'today'){ RANGE.from = todayKey; RANGE.to = todayKey; }
    else if (p === '7'){ RANGE.from = todayKey; RANGE.to = shiftKey(7); }
    else if (p === '30'){ RANGE.from = todayKey; RANGE.to = shiftKey(30); }
    else { RANGE.from = ''; RANGE.to = ''; }
    render();
  });
}

(function(){
  var btn = $('calToggle'), bar = document.querySelector('.calbar');
  function apply(hidden){
    bar.classList.toggle('collapsed', hidden);
    btn.setAttribute('aria-expanded', String(!hidden));
    btn.textContent = hidden ? 'Show calendar' : 'Hide calendar';
    measureFilters();
  }
  var saved = null;
  try { saved = localStorage.getItem('reg-cal'); } catch(e){}
  // on phones the stacked calendar pushes the list far down, so start collapsed
  // unless the reader has said otherwise
  apply(saved ? saved === 'hidden' : window.innerWidth < 760);
  btn.addEventListener('click', function(){
    var nowHidden = !bar.classList.contains('collapsed');
    apply(nowHidden);
    try { localStorage.setItem('reg-cal', nowHidden ? 'hidden' : 'shown'); } catch(e){}
  });
})();

function measureFilters(){
  var bar = document.querySelector('.filters');
  // when the bar is not sticky (narrow screens) day headings pin to the very top
  var stuck = getComputedStyle(bar).position === 'sticky';
  var h = stuck ? (bar.offsetHeight || 76) : 0;
  document.documentElement.style.setProperty('--filterh', h + 'px');
}
window.addEventListener('resize', measureFilters);
measureFilters();

(function(){
  var root = document.documentElement;
  var btns = document.querySelectorAll('[data-theme-set]');
  function paint(){
    var cur = root.getAttribute('data-theme') || 'auto';
    for (var i = 0; i < btns.length; i++){
      btns[i].setAttribute('aria-pressed',
        String(btns[i].getAttribute('data-theme-set') === cur));
    }
  }
  for (var i = 0; i < btns.length; i++){
    btns[i].addEventListener('click', function(){
      var v = this.getAttribute('data-theme-set');
      root.setAttribute('data-theme', v);
      try {
        if (v === 'auto') localStorage.removeItem('reg-theme');
        else localStorage.setItem('reg-theme', v);
      } catch(e){}
      paint();
    });
  }
  try {
    var mq = window.matchMedia('(prefers-color-scheme: dark)');
    var sync = function(ev){ root.classList.toggle('sys-dark', ev.matches); };
    if (mq.addEventListener) mq.addEventListener('change', sync); else mq.addListener(sync);
  } catch(e){}
  paint();
})();

// ---- header: relative "updated" clock + refresh action --------------------
(function(){
  var el = $('updated'), btn = $('refresh');
  var built = el ? new Date(el.getAttribute('data-built')) : null;

  function tick(){
    if (!built || isNaN(built.getTime())) return;
    var secs = Math.round((Date.now() - built.getTime()) / 1000);
    var rel = secs < 60 ? 'just now'
            : secs < 3600 ? Math.round(secs/60) + ' min ago'
            : secs < 86400 ? Math.round(secs/3600) + 'h ago'
            : Math.round(secs/86400) + ' days ago';
    el.textContent = 'harvested ' + rel;
    el.title = built.toString();
  }
  tick();
  setInterval(tick, 30000);

  if (btn){
    btn.addEventListener('click', function(){
      if (btn.getAttribute('aria-busy') === 'true') return;
      btn.setAttribute('aria-busy','true');
      btn.textContent = 'Refreshing\u2026';
      fetch('/refresh').then(function(r){
        if (!r.ok) throw new Error('HTTP ' + r.status);
        location.reload();
      }).catch(function(err){
        btn.removeAttribute('aria-busy');
        btn.textContent = 'Refresh failed';
        el.textContent = 'kept previous data (' + err.message + ')';
        setTimeout(function(){ btn.textContent = 'Refresh now'; tick(); }, 4000);
      });
    });
  }
})();

render();
</script>
</body>
</html>
`
