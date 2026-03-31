package adapter

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/JohannesKaufmann/html-to-markdown/v2/converter"
	"github.com/JohannesKaufmann/html-to-markdown/v2/plugin/base"
	"github.com/JohannesKaufmann/html-to-markdown/v2/plugin/commonmark"
	"github.com/JohannesKaufmann/html-to-markdown/v2/plugin/table"
	"github.com/openwebui-content-sync/internal/config"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/html"
)

// ConfluenceAdapter implements the Adapter interface for Confluence spaces
type ConfluenceAdapter struct {
	client             *http.Client
	config             config.ConfluenceConfig
	lastSync           time.Time
	spaces             []string
	parentPageIDs      []string
	spaceMappings      map[string]string // space_key -> knowledge_id mapping
	parentPageMappings map[string]string // parent_page_id -> knowledge_id mapping
	currentPageID      string            // current page ID for attachment downloads
	cookieJar          http.CookieJar    // shared cookie jar for session authentication
}

// ConfluencePageV1 represents a page from Confluence API v1
type ConfluencePageV1 struct {
	ID      string                 `json:"id"`
	Title   string                 `json:"title"`
	Space   ConfluenceSpaceV1      `json:"space"`
	Body    ConfluenceBodyV1       `json:"body"`
	Version ConfluenceVersionV1    `json:"version"`
	History ConfluenceHistoryV1    `json:"history"`
	Links   map[string]interface{} `json:"_links"`
}

// ConfluenceSpaceV1 represents a space from Confluence API v1
type ConfluenceSpaceV1 struct {
	ID   json.Number `json:"id"`
	Key  string      `json:"key"`
	Name string      `json:"name"`
}

// ConfluenceBodyV1 represents the body content for API v1
type ConfluenceBodyV1 struct {
	Storage ConfluenceBodyStorageV1 `json:"storage"`
}

// ConfluenceBodyStorageV1 represents the storage content for API v1
type ConfluenceBodyStorageV1 struct {
	Value          string `json:"value"`
	Representation string `json:"representation"`
}

// ConfluenceVersionV1 represents version information for API v1
type ConfluenceVersionV1 struct {
	Number int `json:"number"`
}

// ConfluenceHistoryV1 represents history information for API v1
type ConfluenceHistoryV1 struct {
	CreatedBy ConfluenceUserV1 `json:"createdBy"`
	CreatedAt string           `json:"createdAt"`
}

// ConfluenceUserV1 represents a user from Confluence API v1
type ConfluenceUserV1 struct {
	DisplayName string `json:"displayName"`
}

// ConfluencePageListV1 represents the response from listing pages in API v1
type ConfluencePageListV1 struct {
	Results []ConfluencePageV1     `json:"results"`
	Links   map[string]interface{} `json:"_links"`
}

// ConfluenceChildPageListV1 represents the response from listing child pages in API v1
type ConfluenceChildPageListV1 struct {
	Results []ConfluencePageV1     `json:"results"`
	Links   map[string]interface{} `json:"_links"`
}

// ConfluenceSpaceListV1 represents the response from listing spaces in API v1
type ConfluenceSpaceListV1 struct {
	Results []ConfluenceSpaceV1    `json:"results"`
	Links   map[string]interface{} `json:"_links"`
}

// ConfluenceSpace represents a space from Confluence API
type ConfluenceSpace struct {
	ID                 string                 `json:"id"`
	Key                string                 `json:"key"`
	Name               string                 `json:"name"`
	Type               string                 `json:"type"`
	Status             string                 `json:"status"`
	Description        string                 `json:"description"`
	HomepageID         string                 `json:"homepageId"`
	Icon               interface{}            `json:"icon"`
	SpaceOwnerID       string                 `json:"spaceOwnerId"`
	AuthorID           string                 `json:"authorId"`
	CreatedAt          string                 `json:"createdAt"`
	CurrentActiveAlias string                 `json:"currentActiveAlias"`
	Links              map[string]interface{} `json:"_links"`
}

// ConfluenceSpaceList represents the response from listing spaces
type ConfluenceSpaceList struct {
	Results []ConfluenceSpace      `json:"results"`
	Links   map[string]interface{} `json:"_links"`
}

// ConfluencePage represents a page from Confluence API
type ConfluencePage struct {
	ID                string                 `json:"id"`
	Status            string                 `json:"status"`
	Title             string                 `json:"title"`
	SpaceID           string                 `json:"spaceId"`
	ParentID          string                 `json:"parentId"`
	ParentType        string                 `json:"parentType"`
	Position          int                    `json:"position"`
	AuthorID          string                 `json:"authorId"`
	AuthorDisplayName string                 `json:"authorDisplayName"`
	OwnerID           string                 `json:"ownerId"`
	LastOwnerID       string                 `json:"lastOwnerId"`
	CreatedAt         string                 `json:"createdAt"`
	Version           ConfluenceVersion      `json:"version"`
	Body              ConfluenceBody         `json:"body"`
	Links             map[string]interface{} `json:"_links"`
}

// ConfluenceVersion represents version information
type ConfluenceVersion struct {
	CreatedAt string `json:"createdAt"`
	Message   string `json:"message"`
	Number    int    `json:"number"`
	MinorEdit bool   `json:"minorEdit"`
	AuthorID  string `json:"authorId"`
}

// ConfluenceBody represents the body content
type ConfluenceBody struct {
	View       ConfluenceBodyView `json:"view"`
	ExportView ConfluenceBodyView `json:"export_view"`
}

// ConfluenceBodyView represents the view content
type ConfluenceBodyView struct {
	Representation string `json:"representation"`
	Value          string `json:"value"`
}

// ConfluencePageList represents the response from listing pages
type ConfluencePageList struct {
	Results []ConfluencePage       `json:"results"`
	Links   map[string]interface{} `json:"_links"`
}

// ConfluenceChildPage represents a child page from the children API
type ConfluenceChildPage struct {
	ID            string `json:"id"`
	Status        string `json:"status"`
	Title         string `json:"title"`
	SpaceID       string `json:"spaceId"`
	ChildPosition int    `json:"childPosition"`
}

// ConfluenceChildPageList represents the response from listing child pages
type ConfluenceChildPageList struct {
	Results []ConfluenceChildPage  `json:"results"`
	Links   map[string]interface{} `json:"_links"`
}

// ConfluenceAttachment represents an attachment
type ConfluenceAttachment struct {
	ID        string                 `json:"id"`
	Title     string                 `json:"title"`
	MediaType string                 `json:"mediaType"`
	FileSize  int                    `json:"fileSize"`
	Comment   string                 `json:"comment"`
	PageID    string                 `json:"pageId"`
	SpaceID   string                 `json:"spaceId"`
	Version   ConfluenceVersion      `json:"version"`
	CreatedAt string                 `json:"createdAt"`
	AuthorID  string                 `json:"authorId"`
	Links     map[string]interface{} `json:"_links"`
}

// ConfluenceAttachmentList represents the response from listing attachments
type ConfluenceAttachmentList struct {
	Results []ConfluenceAttachment `json:"results"`
	Links   map[string]interface{} `json:"_links"`
}

// ConfluenceBlogPost represents a blog post from Confluence API
type ConfluenceBlogPost struct {
	ID                string                 `json:"id"`
	Status            string                 `json:"status"`
	Title             string                 `json:"title"`
	SpaceID           string                 `json:"spaceId"`
	ParentID          string                 `json:"parentId"`
	ParentType        string                 `json:"parentType"`
	Position          int                    `json:"position"`
	AuthorID          string                 `json:"authorId"`
	AuthorDisplayName string                 `json:"authorDisplayName"`
	OwnerID           string                 `json:"ownerId"`
	LastOwnerID       string                 `json:"lastOwnerId"`
	CreatedAt         string                 `json:"createdAt"`
	Version           ConfluenceVersion      `json:"version"`
	Body              ConfluenceBody         `json:"body"`
	Links             map[string]interface{} `json:"_links"`
}

// ConfluenceBlogPostList represents the response from listing blog posts
type ConfluenceBlogPostList struct {
	Results []ConfluenceBlogPost   `json:"results"`
	Links   map[string]interface{} `json:"_links"`
}

// ConfluenceUser represents a user from Confluence API
type ConfluenceUser struct {
	AccountID        string                     `json:"accountId"`
	AccountType      string                     `json:"accountType"`
	Active           bool                       `json:"active"`
	ApplicationRoles ConfluenceApplicationRoles `json:"applicationRoles"`
	AvatarURLs       map[string]string          `json:"avatarUrls"`
	DisplayName      string                     `json:"displayName"`
	EmailAddress     string                     `json:"emailAddress"`
	Groups           ConfluenceGroups           `json:"groups"`
	Key              string                     `json:"key"`
	Name             string                     `json:"name"`
	Self             string                     `json:"self"`
	TimeZone         string                     `json:"timeZone"`
}

// ConfluenceUserList represents the response from listing users
type ConfluenceUserList struct {
	Results []ConfluenceUser       `json:"results"`
	Links   map[string]interface{} `json:"_links"`
}

// ConfluenceApplicationRoles represents application roles for a user
type ConfluenceApplicationRoles struct {
	Items []interface{} `json:"items"`
	Size  int           `json:"size"`
}

// ConfluenceGroups represents groups for a user
type ConfluenceGroups struct {
	Items []interface{} `json:"items"`
	Size  int           `json:"size"`
}

// NewConfluenceAdapter creates a new Confluence adapter
func NewConfluenceAdapter(cfg config.ConfluenceConfig) (*ConfluenceAdapter, error) {
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("confluence base URL is required")
	}
	if cfg.Username == "" {
		return nil, fmt.Errorf("confluence username is required")
	}
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("confluence API key is required")
	}

	// Build space and parent page mappings
	spaceMappings := make(map[string]string)
	parentPageMappings := make(map[string]string)
	spaces := []string{}
	parentPageIDs := []string{}

	// Process space mappings
	for _, mapping := range cfg.SpaceMappings {
		if mapping.SpaceKey != "" && mapping.KnowledgeID != "" {
			spaceMappings[mapping.SpaceKey] = mapping.KnowledgeID
			spaces = append(spaces, mapping.SpaceKey)
		}
	}

	// Process parent page mappings
	for _, mapping := range cfg.ParentPageMappings {
		if mapping.ParentPageID != "" && mapping.KnowledgeID != "" {
			parentPageMappings[mapping.ParentPageID] = mapping.KnowledgeID
			parentPageIDs = append(parentPageIDs, mapping.ParentPageID)
		}
	}

	// If no mappings are configured, return error
	if len(spaces) == 0 && len(parentPageIDs) == 0 {
		return nil, fmt.Errorf("at least one confluence space or parent page mapping must be configured")
	}

	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create cookie jar: %w", err)
	}

	client := &http.Client{
		Timeout: 60 * time.Second,
		Jar:     jar,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return fmt.Errorf("too many redirects")
			}
			for key, values := range via[0].Header {
				if key != "Authorization" && key != "Cookie" {
					req.Header[key] = values
				}
			}
			logrus.Debugf("Following redirect from %s to %s", via[len(via)-1].URL.String(), req.URL.String())
			return nil
		},
	}

	return &ConfluenceAdapter{
		client:             client,
		config:             cfg,
		spaces:             spaces,
		parentPageIDs:      parentPageIDs,
		spaceMappings:      spaceMappings,
		parentPageMappings: parentPageMappings,
		lastSync:           time.Now(),
		cookieJar:          jar,
	}, nil
}

// loginAndGetSessionCookies logs into Confluence and returns session cookies
func (c *ConfluenceAdapter) loginAndGetSessionCookies() error {
	logrus.Debugf("Attempting to login to Confluence to get session cookies")

	loginClient := &http.Client{
		Timeout: 30 * time.Second,
		Jar:     c.cookieJar,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	loginURL := c.config.BaseURL + "/login.action"
	req, err := http.NewRequestWithContext(context.Background(), "GET", loginURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create login page request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

	resp, err := loginClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to get login page: %w", err)
	}
	resp.Body.Close()
	logrus.Debugf("Got login page, status: %d", resp.StatusCode)

	doLoginURL := c.config.BaseURL + "/dologin.action"
	formData := url.Values{}
	formData.Set("os_username", c.config.Username)
	formData.Set("os_password", c.config.APIKey)
	formData.Set("login", "Log In")
	formData.Set("os_destination", "")
	formData.Set("os_cookie", "true")

	loginReq, err := http.NewRequestWithContext(context.Background(), "POST", doLoginURL, strings.NewReader(formData.Encode()))
	if err != nil {
		return fmt.Errorf("failed to create login request: %w", err)
	}

	loginReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	loginReq.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
	loginReq.Header.Set("Origin", c.config.BaseURL)
	loginReq.Header.Set("Referer", loginURL)

	loginResp, err := loginClient.Do(loginReq)
	if err != nil {
		return fmt.Errorf("login request failed: %w", err)
	}
	defer loginResp.Body.Close()

	logrus.Debugf("Login response status: %d", loginResp.StatusCode)

	u, _ := url.Parse(c.config.BaseURL)
	allCookies := c.cookieJar.Cookies(u)
	if len(allCookies) == 0 {
		return fmt.Errorf("no session cookies received from login")
	}

	var cookieNames []string
	for _, cookie := range allCookies {
		cookieNames = append(cookieNames, cookie.Name)
	}
	logrus.Infof("Login successful, got %d cookies for session authentication: %v", len(allCookies), cookieNames)
	return nil
}

// Name returns the adapter name
func (c *ConfluenceAdapter) Name() string {
	return "confluence"
}

// FetchFiles fetches files from all configured Confluence spaces and parent pages
func (c *ConfluenceAdapter) FetchFiles(ctx context.Context) ([]*File, error) {
	var allFiles []*File

	logrus.Debugf("Confluence adapter config - ParentPageIDs: %v, Spaces: %v, BaseURL: %s, Username: %s",
		c.parentPageIDs, c.spaces, c.config.BaseURL, c.config.Username)

	// Process parent pages if configured
	if len(c.parentPageIDs) > 0 {
		logrus.Debugf("Using PARENT PAGE mode - Processing %d parent pages", len(c.parentPageIDs))
		for _, parentPageID := range c.parentPageIDs {
			logrus.Debugf("Fetching files from Confluence parent page: %s", parentPageID)

			// Step 1: Get the parent page details
			parentPage, err := c.fetchPageByID(ctx, parentPageID)
			if err != nil {
				logrus.Errorf("Failed to fetch parent page %s: %v", parentPageID, err)
				continue
			}

			logrus.Debugf("Parent page: %s (Space: %s)", parentPage.Title, parentPage.SpaceID)

			// Step 2: Fetch all sub-pages under this parent
			pages, err := c.fetchSubPages(ctx, parentPageID)
			if err != nil {
				logrus.Errorf("Failed to fetch sub-pages for parent %s: %v", parentPageID, err)
				continue
			}

			// Include the parent page itself in the results
			pages = append([]ConfluencePage{parentPage}, pages...)

			logrus.Debugf("Found %d pages under parent page %s", len(pages), parentPage.Title)

			// Step 3: Build hierarchical titles and process each page
			knowledgeID := c.parentPageMappings[parentPageID]
			titleMap := c.buildHierarchicalTitles(pages)
			for _, page := range pages {
				pageCopy := page
				if fullTitle, ok := titleMap[page.ID]; ok && fullTitle != "" {
					pageCopy.Title = fullTitle
				}

				file, err := c.processPage(ctx, pageCopy, knowledgeID)
				if err != nil {
					logrus.Errorf("Failed to process page %s: %v", page.Title, err)
					continue
				}
				allFiles = append(allFiles, file)
			}
		}
	}

	// Process spaces if configured
	if len(c.spaces) > 0 {
		logrus.Debugf("Using SPACE mode - Processing %d spaces", len(c.spaces))
		for _, spaceKey := range c.spaces {
			logrus.Debugf("Fetching files from Confluence space: %s", spaceKey)

			// Step 1: Get space ID from space key
			spaceID, err := c.getSpaceID(ctx, spaceKey)
			if err != nil {
				logrus.Errorf("Failed to get space ID for %s: %v", spaceKey, err)
				continue
			}

			logrus.Debugf("Space %s has ID: %s", spaceKey, spaceID)

			// Step 2: Fetch pages from the space
			pages, err := c.fetchSpacePages(ctx, spaceID, spaceKey)
			if err != nil {
				logrus.Errorf("Failed to fetch pages from space %s: %v", spaceKey, err)
				continue
			}

			logrus.Debugf("Found %d pages in space %s", len(pages), spaceKey)

			// Step 3: Build hierarchical titles and process each page
			knowledgeID := c.spaceMappings[spaceKey]
			titleMap := c.buildHierarchicalTitles(pages)
			for _, page := range pages {
				pageCopy := page
				if fullTitle, ok := titleMap[page.ID]; ok && fullTitle != "" {
					pageCopy.Title = fullTitle
				}

				file, err := c.processPage(ctx, pageCopy, knowledgeID)
				if err != nil {
					logrus.Errorf("Failed to process page %s: %v", page.Title, err)
					continue
				}
				allFiles = append(allFiles, file)
			}

			// Step 4: Fetch blog posts from the space
			if c.config.IncludeBlogPosts {
				blogposts, err := c.fetchSpaceBlogposts(ctx, spaceID)
				if err != nil {
					logrus.Errorf("Failed to fetch blog posts from space %s: %v", spaceKey, err)
					continue
				}

				logrus.Debugf("Found %d blog posts in space %s", len(blogposts), spaceKey)

				// Step 5: Process each blog post
				for _, blogpost := range blogposts {
					file, err := c.processBlogpost(ctx, blogpost, knowledgeID)
					if err != nil {
						logrus.Errorf("Failed to process blog post %s: %v", blogpost.Title, err)
						continue
					}
					allFiles = append(allFiles, file)
				}
			}
		}
	}

	c.lastSync = time.Now()
	return allFiles, nil
}

// getSpaceID retrieves the space ID from the space key
func (c *ConfluenceAdapter) getSpaceID(ctx context.Context, spaceKey string) (string, error) {
	// URL encode the space key
	encodedSpaceKey := url.QueryEscape(spaceKey)
	var url string
	var spaceID string

	if c.config.APIVersion == "v1" {
		// API v1 endpoint
		url = fmt.Sprintf("%s/rest/api/space/%s", c.config.BaseURL, encodedSpaceKey)
	} else {
		// API v2 endpoint (default)
		url = fmt.Sprintf("%s/api/v2/spaces?keys=%s", c.config.BaseURL, encodedSpaceKey)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	// Set authentication
	req.SetBasicAuth(c.config.Username, c.config.APIKey)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "OpenWebUI-Content-Sync/1.0")

	logrus.Debugf("Confluence space API URL: %s", url)
	logrus.Debugf("Confluence space key - Original: %s, Encoded: %s", spaceKey, encodedSpaceKey)
	logrus.Debugf("Confluence auth - Username: %s, APIKey length: %d", c.config.Username, len(c.config.APIKey))
	logrus.Debugf("Request headers: %+v", req.Header)

	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body) // Consume body for proper connection reuse
		logrus.Errorf("Confluence space API failed - Status: %d, URL: %s, Response: %s", resp.StatusCode, url, string(body))
		return "", fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	if c.config.APIVersion == "v1" {
		// API v1 response parsing
		var space ConfluenceSpaceV1
		if err := json.NewDecoder(resp.Body).Decode(&space); err != nil {
			return "", fmt.Errorf("failed to decode response: %w", err)
		}
		spaceID = space.ID.String()
	} else {
		// API v2 response parsing
		var spaceList ConfluenceSpaceList
		if err := json.NewDecoder(resp.Body).Decode(&spaceList); err != nil {
			return "", fmt.Errorf("failed to decode response: %w", err)
		}

		if len(spaceList.Results) == 0 {
			return "", fmt.Errorf("space %s not found", spaceKey)
		}

		spaceID = spaceList.Results[0].ID
	}

	return spaceID, nil
}

// fetchSpacePages fetches all pages from a space using space ID or space key
func (c *ConfluenceAdapter) fetchSpacePages(ctx context.Context, spaceID string, spaceKey string) ([]ConfluencePage, error) {
	if c.config.APIVersion == "v1" {
		return c.fetchSpacePagesV1(ctx, spaceKey)
	}
	return c.fetchSpacePagesV2(ctx, spaceID)
}

// fetchSpacePagesV2 fetches all pages from a space using space ID with API v2
func (c *ConfluenceAdapter) fetchSpacePagesV2(ctx context.Context, spaceID string) ([]ConfluencePage, error) {
	var allPages []ConfluencePage
	maxPages := c.config.PageLimit // 0 means no global limit
	requestLimit := 100
	if maxPages > 0 && maxPages < requestLimit {
		requestLimit = maxPages
	}

	url := fmt.Sprintf("%s/api/v2/spaces/%s/pages?limit=%d", c.config.BaseURL, spaceID, requestLimit)

	for {
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		// Set authentication
		req.SetBasicAuth(c.config.Username, c.config.APIKey)
		req.Header.Set("Accept", "application/json")

		logrus.Debugf("Confluence pages API URL: %s", url)

		resp, err := c.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to make request: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("API request failed with status %d: response body omitted", resp.StatusCode)
		}

		var pageList ConfluencePageList
		if err := json.NewDecoder(resp.Body).Decode(&pageList); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}
		resp.Body.Close()

		if maxPages > 0 {
			remaining := maxPages - len(allPages)
			if remaining <= 0 {
				break
			}
			if len(pageList.Results) > remaining {
				allPages = append(allPages, pageList.Results[:remaining]...)
			} else {
				allPages = append(allPages, pageList.Results...)
			}
		} else {
			allPages = append(allPages, pageList.Results...)
		}

		if maxPages > 0 && len(allPages) >= maxPages {
			break
		}

		// Check for next page
		nextLink, hasNext := pageList.Links["next"]
		if !hasNext {
			break
		}

		nextURL, ok := nextLink.(string)
		if !ok {
			break
		}
		// Check if nextURL doesn't start with https
		if nextURL != "" && !strings.HasPrefix(nextURL, "https") {
			// Prepend the base URL
			nextURL = c.config.BaseURL + nextURL
		}

		url = nextURL
	}

	// Extract all unique AuthorIDs from pages
	authorIDs := make(map[string]bool)
	for _, page := range allPages {
		if page.AuthorID != "" {
			authorIDs[page.AuthorID] = true
		}
	}

	// If we have author IDs and additional data is enabled, fetch user information
	if c.config.AddAdditionalData && len(authorIDs) > 0 {
		// Convert map keys to slice
		accountIDs := make([]string, 0, len(authorIDs))
		for accountID := range authorIDs {
			accountIDs = append(accountIDs, accountID)
		}

		// Fetch users by IDs
		users, err := c.fetchUsersByIds(ctx, accountIDs)
		if err != nil {
			logrus.Errorf("Failed to fetch users for pages: %v", err)
			// Continue without user information if fetch fails
		} else {
			// Update pages with user display names
			for i := range allPages {
				if user, exists := users[allPages[i].AuthorID]; exists {
					allPages[i].AuthorDisplayName = user.DisplayName
				}
			}
		}
	}

	return allPages, nil
}

// fetchSpacePagesV1 fetches all pages from a space using space key with API v1
func (c *ConfluenceAdapter) fetchSpacePagesV1(ctx context.Context, spaceKey string) ([]ConfluencePage, error) {
	var allPages []ConfluencePage
	maxPages := c.config.PageLimit // 0 means no global limit
	requestLimit := 100
	if maxPages > 0 && maxPages < requestLimit {
		requestLimit = maxPages
	}

	url := fmt.Sprintf("%s/rest/api/content?spaceKey=%s&type=page&limit=%d", c.config.BaseURL, url.QueryEscape(spaceKey), requestLimit)

	for {
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		// Set authentication
		req.SetBasicAuth(c.config.Username, c.config.APIKey)
		req.Header.Set("Accept", "application/json")

		logrus.Debugf("Confluence pages API v1 URL: %s", url)

		resp, err := c.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to make request: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("API request failed with status %d: response body omitted", resp.StatusCode)
		}

		var pageList ConfluencePageListV1
		if err := json.NewDecoder(resp.Body).Decode(&pageList); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}
		resp.Body.Close()

		// Convert v1 pages to v2 format
		for _, v1Page := range pageList.Results {
			if maxPages > 0 && len(allPages) >= maxPages {
				break
			}
			page := ConfluencePage{
				ID:                v1Page.ID,
				Status:            "current",
				Title:             v1Page.Title,
				SpaceID:           v1Page.Space.ID.String(),
				AuthorID:          "", // API v1 doesn't provide account ID
				AuthorDisplayName: v1Page.History.CreatedBy.DisplayName,
				CreatedAt:         v1Page.History.CreatedAt,
				Version: ConfluenceVersion{
					Number: v1Page.Version.Number,
				},
				Body: ConfluenceBody{
					View: ConfluenceBodyView{
						Value: v1Page.Body.Storage.Value,
					},
				},
				Links: v1Page.Links,
			}
			allPages = append(allPages, page)
		}

		if maxPages > 0 && len(allPages) >= maxPages {
			break
		}

		// Check for next page
		nextLink, hasNext := pageList.Links["next"]
		if !hasNext {
			break
		}

		nextURL, ok := nextLink.(string)
		if !ok {
			break
		}
		// Check if nextURL doesn't start with https
		if nextURL != "" && !strings.HasPrefix(nextURL, "https") {
			// Prepend the base URL
			nextURL = c.config.BaseURL + nextURL
		}

		url = nextURL
	}

	return allPages, nil
}

// fetchPageByID fetches a specific page by its ID
func (c *ConfluenceAdapter) fetchPageByID(ctx context.Context, pageID string) (ConfluencePage, error) {
	if c.config.APIVersion == "v1" {
		return c.fetchPageByIDV1(ctx, pageID)
	}
	return c.fetchPageByIDV2(ctx, pageID)
}

// fetchPageByIDV2 fetches a specific page by its ID using API v2
func (c *ConfluenceAdapter) fetchPageByIDV2(ctx context.Context, pageID string) (ConfluencePage, error) {
	url := fmt.Sprintf("%s/api/v2/pages/%s", c.config.BaseURL, pageID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return ConfluencePage{}, fmt.Errorf("failed to create request: %w", err)
	}

	// Set authentication
	req.SetBasicAuth(c.config.Username, c.config.APIKey)
	req.Header.Set("Accept", "application/json")

	logrus.Debugf("Confluence page API URL: %s", url)
	resp, err := c.client.Do(req)
	if err != nil {
		return ConfluencePage{}, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body) // Consume body for proper connection reuse
		logrus.Errorf("Confluence page API failed - Status: %d, URL: %s, Response: %s", resp.StatusCode, url, string(body))
		return ConfluencePage{}, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var page ConfluencePage
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		return ConfluencePage{}, fmt.Errorf("failed to decode response: %w", err)
	}

	return page, nil
}

// fetchPageByIDV1 fetches a specific page by its ID using API v1
func (c *ConfluenceAdapter) fetchPageByIDV1(ctx context.Context, pageID string) (ConfluencePage, error) {
	url := fmt.Sprintf("%s/rest/api/content/%s?expand=body.storage,history.createdBy,space", c.config.BaseURL, pageID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return ConfluencePage{}, fmt.Errorf("failed to create request: %w", err)
	}

	// Set authentication
	req.SetBasicAuth(c.config.Username, c.config.APIKey)
	req.Header.Set("Accept", "application/json")

	logrus.Debugf("Confluence page API v1 URL: %s", url)
	resp, err := c.client.Do(req)
	if err != nil {
		return ConfluencePage{}, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body) // Consume body for proper connection reuse
		logrus.Errorf("Confluence page API v1 failed - Status: %d, URL: %s, Response: %s", resp.StatusCode, url, string(body))
		return ConfluencePage{}, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var v1Page ConfluencePageV1
	if err := json.NewDecoder(resp.Body).Decode(&v1Page); err != nil {
		return ConfluencePage{}, fmt.Errorf("failed to decode response: %w", err)
	}

	// Convert v1 page to v2 format
	page := ConfluencePage{
		ID:                v1Page.ID,
		Status:            "current",
		Title:             v1Page.Title,
		SpaceID:           v1Page.Space.ID.String(),
		AuthorID:          "", // API v1 doesn't provide account ID
		AuthorDisplayName: v1Page.History.CreatedBy.DisplayName,
		CreatedAt:         v1Page.History.CreatedAt,
		Version: ConfluenceVersion{
			Number: v1Page.Version.Number,
		},
		Body: ConfluenceBody{
			View: ConfluenceBodyView{
				Value: v1Page.Body.Storage.Value,
			},
		},
		Links: v1Page.Links,
	}

	return page, nil
}

// fetchSubPages fetches all sub-pages under a specific parent page
func (c *ConfluenceAdapter) fetchSubPages(ctx context.Context, parentPageID string) ([]ConfluencePage, error) {
	if c.config.APIVersion == "v1" {
		return c.fetchSubPagesV1(ctx, parentPageID)
	}
	return c.fetchSubPagesV2(ctx, parentPageID)
}

// fetchSubPagesV2 fetches all sub-pages under a specific parent page using API v2 (recursively)
func (c *ConfluenceAdapter) fetchSubPagesV2(ctx context.Context, parentPageID string) ([]ConfluencePage, error) {
	var total int
	return c.fetchSubPagesV2Recursive(ctx, parentPageID, &total)
}

// fetchSubPagesV2Recursive recursively fetches all sub-pages under a parent page using API v2
func (c *ConfluenceAdapter) fetchSubPagesV2Recursive(ctx context.Context, parentPageID string, total *int) ([]ConfluencePage, error) {
	var allPages []ConfluencePage
	maxPages := c.config.PageLimit // 0 means no global limit
	requestLimit := 100
	if maxPages > 0 && maxPages < requestLimit {
		requestLimit = maxPages
	}

	url := fmt.Sprintf("%s/api/v2/pages/%s/children?limit=%d", c.config.BaseURL, parentPageID, requestLimit)

	for {
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		req.SetBasicAuth(c.config.Username, c.config.APIKey)
		req.Header.Set("Accept", "application/json")

		logrus.Debugf("Confluence sub-pages API URL: %s", url)

		resp, err := c.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to make request: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("API request failed with status %d: response body omitted", resp.StatusCode)
		}

		var childPageList ConfluenceChildPageList
		if err := json.NewDecoder(resp.Body).Decode(&childPageList); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}
		resp.Body.Close()

		for _, childPage := range childPageList.Results {
			if maxPages > 0 && *total >= maxPages {
				break
			}
			if maxPages > 0 && *total >= maxPages {
				break
			}
			if maxPages > 0 && *total >= maxPages {
				break
			}

			fullPage, err := c.fetchPageByID(ctx, childPage.ID)
			if err != nil {
				logrus.Errorf("Failed to fetch full page details for %s: %v", childPage.ID, err)
				continue
			}
			// Ensure parent relationship is available for hierarchical naming
			fullPage.ParentID = parentPageID
			allPages = append(allPages, fullPage)
			*total++

			if maxPages > 0 && *total >= maxPages {
				continue
			}

			subPages, err := c.fetchSubPagesV2Recursive(ctx, childPage.ID, total)
			if err != nil {
				logrus.Warnf("Failed to fetch sub-pages for %s: %v", childPage.ID, err)
			} else {
				allPages = append(allPages, subPages...)
			}
		}

		if maxPages > 0 && *total >= maxPages {
			break
		}

		nextLink, hasNext := childPageList.Links["next"]
		if !hasNext {
			break
		}

		nextURL, ok := nextLink.(string)
		if !ok {
			break
		}
		if nextURL != "" && !strings.HasPrefix(nextURL, "https") {
			nextURL = c.config.BaseURL + nextURL
		}
		url = nextURL
	}

	return allPages, nil
}

// fetchSubPagesV1 fetches all sub-pages under a specific parent page using API v1 (recursively)
func (c *ConfluenceAdapter) fetchSubPagesV1(ctx context.Context, parentPageID string) ([]ConfluencePage, error) {
	var total int
	return c.fetchSubPagesV1Recursive(ctx, parentPageID, &total)
}

// fetchSubPagesV1Recursive recursively fetches all sub-pages under a parent page
func (c *ConfluenceAdapter) fetchSubPagesV1Recursive(ctx context.Context, parentPageID string, total *int) ([]ConfluencePage, error) {
	var allPages []ConfluencePage
	maxPages := c.config.PageLimit // 0 means no global limit
	requestLimit := 100
	if maxPages > 0 && maxPages < requestLimit {
		requestLimit = maxPages
	}

	url := fmt.Sprintf("%s/rest/api/content/%s/child/page?limit=%d", c.config.BaseURL, parentPageID, requestLimit)

	for {
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		req.SetBasicAuth(c.config.Username, c.config.APIKey)
		req.Header.Set("Accept", "application/json")

		logrus.Debugf("Confluence sub-pages API v1 URL: %s", url)

		resp, err := c.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to make request: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("API request failed with status %d: response body omitted", resp.StatusCode)
		}

		var childPageList ConfluenceChildPageListV1
		if err := json.NewDecoder(resp.Body).Decode(&childPageList); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}
		resp.Body.Close()

		for _, childPage := range childPageList.Results {
			if maxPages > 0 && *total >= maxPages {
				break
			}

			page := ConfluencePage{
				ID:                childPage.ID,
				Status:            "current",
				Title:             childPage.Title,
				SpaceID:           childPage.Space.ID.String(),
				AuthorID:          "",
				AuthorDisplayName: childPage.History.CreatedBy.DisplayName,
				CreatedAt:         childPage.History.CreatedAt,
				Version: ConfluenceVersion{
					Number: childPage.Version.Number,
				},
				Body: ConfluenceBody{
					View: ConfluenceBodyView{
						Value: childPage.Body.Storage.Value,
					},
				},
				Links: childPage.Links,
			}
			// Ensure parent relationship is available for hierarchical naming
			page.ParentID = parentPageID
			allPages = append(allPages, page)
			*total++

			if maxPages > 0 && *total >= maxPages {
				continue
			}

			subPages, err := c.fetchSubPagesV1Recursive(ctx, childPage.ID, total)
			if err != nil {
				logrus.Warnf("Failed to fetch sub-pages for %s: %v", childPage.ID, err)
			} else {
				allPages = append(allPages, subPages...)
			}
		}

		if maxPages > 0 && *total >= maxPages {
			break
		}

		nextLink, hasNext := childPageList.Links["next"]
		if !hasNext {
			break
		}

		nextURL, ok := nextLink.(string)
		if !ok {
			break
		}
		if nextURL != "" && !strings.HasPrefix(nextURL, "https") {
			nextURL = c.config.BaseURL + nextURL
		}
		url = nextURL
	}

	return allPages, nil
}

// processPage processes a single page and returns a File
func (c *ConfluenceAdapter) processPage(ctx context.Context, page ConfluencePage, knowledgeID string) (*File, error) {
	c.currentPageID = page.ID

	var fileContent []byte
	var filename string
	var fallbackContent []byte
	var fallbackFilename string

	generateMarkdownContent := func() ([]byte, string, error) {
		pageBody, err := c.fetchPageBody(ctx, page.ID)
		if err != nil {
			return nil, "", fmt.Errorf("failed to fetch page body: %w", err)
		}
		mdFilename := c.generateUniqueFilename(page.Title, page.ID, c.config.UseMarkdownParser)
		webuiLink := ""
		if webui, exists := page.Links["webui"]; exists {
			if webuiStr, ok := webui.(string); ok {
				webuiLink = webuiStr
			}
		}
		metaData := fmt.Sprintf("---\nAuthor: %s\nCreatedAt: %s\nLinkToPage: %s\nTitle: %s\nPageID: %s\n---", page.AuthorDisplayName, page.CreatedAt, c.config.BaseURL+"/wiki"+webuiLink, page.Title, page.ID)
		content := fmt.Sprintf("%s\n\n%s", metaData, pageBody)
		return []byte(content), mdFilename, nil
	}

	if c.config.ExportAsPDF {
		pdfData, err := c.exportPageAsPDF(ctx, page.ID)
		if err != nil {
			logrus.Warnf("Failed to export page %s as PDF, falling back to markdown: %v", page.Title, err)
			fileContent, filename, err = generateMarkdownContent()
			if err != nil {
				return nil, err
			}
		} else {
			filename = c.generateUniqueFilename(page.Title, page.ID, true)
			filename = strings.TrimSuffix(filename, ".md") + ".pdf"
			fileContent = pdfData
			var fbErr error
			fallbackContent, fallbackFilename, fbErr = generateMarkdownContent()
			if fbErr != nil {
				// 降级为 info 级别，因为 PDF 已经成功导出，markdown fallback 只是可选功能
				logrus.Infof("Markdown fallback not available for page %s (PDF export succeeded)", page.Title)
			}
		}
	} else {
		var mdErr error
		fileContent, filename, mdErr = generateMarkdownContent()
		if mdErr != nil {
			return nil, mdErr
		}
	}

	// 检查文件内容是否为空（跳过而不是报错）
	if len(fileContent) == 0 {
		logrus.Infof("Skipping page %s (ID: %s): content is empty", page.Title, page.ID)
		return nil, nil
	}

	hash := sha256.Sum256(fileContent)
	contentHash := base64.StdEncoding.EncodeToString(hash[:])

	logrus.Debugf("Generated file content for %s: %d bytes", filename, len(fileContent))

	return &File{
		Path:              filename,
		Content:           fileContent,
		Hash:              contentHash,
		Modified:          c.lastSync,
		Size:              int64(len(fileContent)),
		Source:            "confluence",
		KnowledgeID:       knowledgeID,
		FallbackPath:      fallbackFilename,
		FallbackContent:   fallbackContent,
		ConfluenceVersion: page.Version.Number, // 添加版本号
		PageID:            page.ID,             // 添加页面 ID
	}, nil
}

// exportPageAsPDF exports a Confluence page as PDF
func (c *ConfluenceAdapter) exportPageAsPDF(ctx context.Context, pageID string) ([]byte, error) {
	u, _ := url.Parse(c.config.BaseURL)
	cookies := c.cookieJar.Cookies(u)
	if len(cookies) == 0 {
		if err := c.loginAndGetSessionCookies(); err != nil {
			return nil, fmt.Errorf("failed to login for PDF export: %w", err)
		}
	}

	pdfURL := fmt.Sprintf("%s/spaces/flyingpdf/pdfpageexport.action?pageId=%s", c.config.BaseURL, pageID)
	logrus.Debugf("Exporting page as PDF: %s", pdfURL)

	req, err := http.NewRequestWithContext(ctx, "GET", pdfURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create PDF export request: %w", err)
	}

	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
	req.Header.Set("Accept", "application/pdf,*/*")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("PDF export request failed: %w", err)
	}
	defer resp.Body.Close()

	logrus.Debugf("PDF export response status: %d, Content-Type: %s", resp.StatusCode, resp.Header.Get("Content-Type"))

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("PDF export failed with status %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if strings.Contains(contentType, "text/html") {
		return nil, fmt.Errorf("PDF export returned HTML (likely auth redirect)")
	}

	pdfData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read PDF data: %w", err)
	}

	logrus.Infof("Successfully exported page %s as PDF (%d bytes)", pageID, len(pdfData))
	return pdfData, nil
}

// fetchPageBody fetches the body content of a specific page
func (c *ConfluenceAdapter) fetchPageBody(ctx context.Context, pageID string) (string, error) {
	if c.config.APIVersion == "v1" {
		return c.fetchPageBodyV1(ctx, pageID)
	}
	return c.fetchPageBodyV2(ctx, pageID)
}

// fetchPageBodyV2 fetches the body content of a specific page using API v2
func (c *ConfluenceAdapter) fetchPageBodyV2(ctx context.Context, pageID string) (string, error) {
	url := fmt.Sprintf("%s/api/v2/pages/%s?body-format=export_view", c.config.BaseURL, pageID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	// Set authentication
	req.SetBasicAuth(c.config.Username, c.config.APIKey)
	req.Header.Set("Accept", "application/json")

	logrus.Debugf("Confluence page body API URL: %s", url)

	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API request failed with status %d: response body omitted", resp.StatusCode)
	}

	var page ConfluencePage
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}
	// Extract content from body.view.value
	if page.Body.ExportView.Value != "" {
		// Convert HTML to plain text or markdown based on configuration
		if c.config.UseMarkdownParser {
			return c.HtmlToMarkdown(page.Body.ExportView.Value), nil
		}
		return c.HtmlToText(page.Body.ExportView.Value), nil
	}

	return "", fmt.Errorf("no content found in page body")
}

// fetchPageBodyV1 fetches the body content of a specific page using API v1
func (c *ConfluenceAdapter) fetchPageBodyV1(ctx context.Context, pageID string) (string, error) {
	url := fmt.Sprintf("%s/rest/api/content/%s?expand=body.storage", c.config.BaseURL, pageID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	// Set authentication
	req.SetBasicAuth(c.config.Username, c.config.APIKey)
	req.Header.Set("Accept", "application/json")

	logrus.Debugf("Confluence page body API v1 URL: %s", url)

	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API request failed with status %d: response body omitted", resp.StatusCode)
	}

	var page ConfluencePageV1
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}
	// Extract content from body.storage.value
	if page.Body.Storage.Value != "" {
		// Convert HTML to plain text or markdown based on configuration
		if c.config.UseMarkdownParser {
			return c.HtmlToMarkdown(page.Body.Storage.Value), nil
		}
		return c.HtmlToText(page.Body.Storage.Value), nil
	}

	return "", fmt.Errorf("no content found in page body")
}

// fetchSpaceBlogposts fetches all blog posts from a space using space ID
func (c *ConfluenceAdapter) fetchSpaceBlogposts(ctx context.Context, spaceID string) ([]ConfluenceBlogPost, error) {
	var allBlogposts []ConfluenceBlogPost
	maxPages := c.config.PageLimit // 0 means no global limit
	requestLimit := 100
	if maxPages > 0 && maxPages < requestLimit {
		requestLimit = maxPages
	}

	url := fmt.Sprintf("%s/api/v2/spaces/%s/blogposts?limit=%d", c.config.BaseURL, spaceID, requestLimit)

	for {
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		// Set authentication
		req.SetBasicAuth(c.config.Username, c.config.APIKey)
		req.Header.Set("Accept", "application/json")

		logrus.Debugf("Confluence blogposts API URL: %s", url)

		resp, err := c.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to make request: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("API request failed with status %d: response body omitted", resp.StatusCode)
		}

		var blogpostList ConfluenceBlogPostList
		if err := json.NewDecoder(resp.Body).Decode(&blogpostList); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}
		resp.Body.Close()

		if maxPages > 0 {
			remaining := maxPages - len(allBlogposts)
			if remaining <= 0 {
				break
			}
			if len(blogpostList.Results) > remaining {
				allBlogposts = append(allBlogposts, blogpostList.Results[:remaining]...)
			} else {
				allBlogposts = append(allBlogposts, blogpostList.Results...)
			}
		} else {
			allBlogposts = append(allBlogposts, blogpostList.Results...)
		}

		if maxPages > 0 && len(allBlogposts) >= maxPages {
			break
		}

		// Check for next page
		nextLink, hasNext := blogpostList.Links["next"]
		if !hasNext {
			break
		}

		nextURL, ok := nextLink.(string)
		if !ok {
			break
		}
		// Check if nextURL doesn't start with https
		if nextURL != "" && !strings.HasPrefix(nextURL, "https") {
			// Prepend the base URL
			nextURL = c.config.BaseURL + nextURL
		}

		url = nextURL
	}

	// Extract all unique AuthorIDs from blogposts
	authorIDs := make(map[string]bool)
	for _, blogpost := range allBlogposts {
		if blogpost.AuthorID != "" {
			authorIDs[blogpost.AuthorID] = true
		}
	}

	// If we have author IDs and additional data is enabled, fetch user information
	if c.config.AddAdditionalData && len(authorIDs) > 0 {
		// Convert map keys to slice
		accountIDs := make([]string, 0, len(authorIDs))
		for accountID := range authorIDs {
			accountIDs = append(accountIDs, accountID)
		}

		// Fetch users by IDs
		users, err := c.fetchUsersByIds(ctx, accountIDs)
		if err != nil {
			logrus.Errorf("Failed to fetch users for blogposts: %v", err)
			// Continue without user information if fetch fails
		} else {
			// Update blogposts with user display names
			for i := range allBlogposts {
				if user, exists := users[allBlogposts[i].AuthorID]; exists {
					allBlogposts[i].AuthorDisplayName = user.DisplayName
				}
			}
		}
	}

	return allBlogposts, nil
}

// fetchBlogpostByID fetches a specific blog post by its ID
func (c *ConfluenceAdapter) fetchBlogpostByID(ctx context.Context, blogpostID string) (ConfluenceBlogPost, error) {
	url := fmt.Sprintf("%s/api/v2/blogposts/%s", c.config.BaseURL, blogpostID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return ConfluenceBlogPost{}, fmt.Errorf("failed to create request: %w", err)
	}

	// Set authentication
	req.SetBasicAuth(c.config.Username, c.config.APIKey)
	req.Header.Set("Accept", "application/json")

	logrus.Debugf("Confluence blogpost API URL: %s", url)
	resp, err := c.client.Do(req)
	if err != nil {
		return ConfluenceBlogPost{}, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body) // Consume body for proper connection reuse
		logrus.Errorf("Confluence blogpost API failed - Status: %d, URL: %s, Response: %s", resp.StatusCode, url, string(body))
		return ConfluenceBlogPost{}, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var blogpost ConfluenceBlogPost
	if err := json.NewDecoder(resp.Body).Decode(&blogpost); err != nil {
		return ConfluenceBlogPost{}, fmt.Errorf("failed to decode response: %w", err)
	}

	return blogpost, nil
}

// processBlogpost processes a single blog post and returns a File
func (c *ConfluenceAdapter) processBlogpost(ctx context.Context, blogpost ConfluenceBlogPost, knowledgeID string) (*File, error) {
	c.currentPageID = blogpost.ID

	blogpostBody, err := c.fetchBlogpostBody(ctx, blogpost.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch blogpost body: %w", err)
	}

	filename := c.generateUniqueFilename(blogpost.Title, blogpost.ID, c.config.UseMarkdownParser)

	webuiLink := ""
	if webui, exists := blogpost.Links["webui"]; exists {
		if webuiStr, ok := webui.(string); ok {
			webuiLink = webuiStr
		}
	}
	metaData := fmt.Sprintf("---\nAuthor: %s\nCreatedAt: %s\nLinkToPage: %s\nTitle: %s\nBlogpostID: %s\n---", blogpost.AuthorDisplayName, blogpost.CreatedAt, c.config.BaseURL+"/wiki"+webuiLink, blogpost.Title, blogpost.ID)

	content := fmt.Sprintf("%s\n\n%s", metaData, blogpostBody)

	fileContent := []byte(content)

	hash := sha256.Sum256(fileContent)
	contentHash := base64.StdEncoding.EncodeToString(hash[:])

	return &File{
		Path:        filename,
		Content:     fileContent,
		Hash:        contentHash,
		Modified:    c.lastSync,
		Size:        int64(len(fileContent)),
		Source:      "confluence",
		KnowledgeID: knowledgeID,
	}, nil
}

// fetchBlogpostBody fetches the body content of a specific blog post
func (c *ConfluenceAdapter) fetchBlogpostBody(ctx context.Context, blogpostID string) (string, error) {
	url := fmt.Sprintf("%s/api/v2/blogposts/%s?body-format=export_view", c.config.BaseURL, blogpostID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	// Set authentication
	req.SetBasicAuth(c.config.Username, c.config.APIKey)
	req.Header.Set("Accept", "application/json")

	logrus.Debugf("Confluence blogpost body API URL: %s", url)

	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API request failed with status %d: response body omitted", resp.StatusCode)
	}

	var blogpost ConfluenceBlogPost
	if err := json.NewDecoder(resp.Body).Decode(&blogpost); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}
	// Extract content from body.view.value
	if blogpost.Body.ExportView.Value != "" {
		// Convert HTML to plain text or markdown based on configuration
		if c.config.UseMarkdownParser {
			return c.HtmlToMarkdown(blogpost.Body.ExportView.Value), nil
		}
		return c.HtmlToText(blogpost.Body.ExportView.Value), nil
	}

	return "", fmt.Errorf("no content found in blogpost body")
}

// HtmlToMarkdown converts HTML content to markdown with embedded images
func (c *ConfluenceAdapter) HtmlToMarkdown(htmlContent string) string {
	htmlContent = c.preprocessConfluenceImages(htmlContent)

	conv := converter.NewConverter(
		converter.WithPlugins(
			base.NewBasePlugin(),
			commonmark.NewCommonmarkPlugin(
				commonmark.WithStrongDelimiter("__"),
			),
			table.NewTablePlugin(),
		),
	)

	conv.Register.RendererFor("img", converter.TagTypeInline, func(ctx converter.Context, w converter.Writer, node *html.Node) converter.RenderStatus {
		src := ""
		alt := ""
		for _, attr := range node.Attr {
			if attr.Key == "src" {
				src = attr.Val
			} else if attr.Key == "alt" {
				alt = attr.Val
			}
		}

		if src != "" {
			originalSrc := src
			if !strings.HasPrefix(src, "http://") && !strings.HasPrefix(src, "https://") {
				src = c.config.BaseURL + src
			}
			logrus.Debugf("Processing image: original=%s, full=%s", originalSrc, src)

			imgMarkdown := c.downloadAndEmbedImage(src, alt)
			w.WriteString(imgMarkdown)
			return converter.RenderSuccess
		}
		return converter.RenderTryNext
	}, 100)

	markdown, err := conv.ConvertString(htmlContent)
	if err != nil {
		logrus.Warnf("Failed to convert HTML to markdown: %v", err)
		return htmlContent
	}
	return markdown
}

// downloadAndEmbedImage downloads an image and returns a base64 embedded markdown image
func (c *ConfluenceAdapter) downloadAndEmbedImage(src, alt string) string {
	src = strings.TrimSpace(src)
	src = strings.Trim(src, "`'\"")
	src = strings.TrimSuffix(src, ")")
	src = strings.TrimSuffix(src, "`")

	if src == "" {
		logrus.Debugf("Empty image source after cleanup")
		return ""
	}

	logrus.Infof("Downloading image: %s", src)

	imgMarkdown := fmt.Sprintf("![%s](%s)", alt, src)

	req, err := http.NewRequestWithContext(context.Background(), "GET", src, nil)
	if err != nil {
		logrus.Warnf("Failed to create image request for %s: %v", src, err)
		return imgMarkdown
	}

	if c.config.PersonalAccessToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.config.PersonalAccessToken)
		logrus.Debugf("Using PAT authentication for image download")
	} else {
		logrus.Debugf("Using session cookie authentication for image download (via cookie jar)")
	}
	req.Header.Set("Accept", "image/*")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
	req.Header.Set("X-Atlassian-Token", "no-check")

	resp, err := c.client.Do(req)
	if err != nil {
		logrus.Warnf("Failed to download image %s: %v", src, err)
		return imgMarkdown
	}
	defer resp.Body.Close()

	logrus.Debugf("Image response status: %d, Content-Type: %s", resp.StatusCode, resp.Header.Get("Content-Type"))

	if resp.StatusCode == http.StatusFound || resp.StatusCode == http.StatusSeeOther {
		location := resp.Header.Get("Location")
		if location != "" {
			logrus.Debugf("Image redirected to: %s", location)
			if !strings.HasPrefix(location, "http") {
				location = c.config.BaseURL + location
			}
			return c.downloadAndEmbedImage(location, alt)
		}
	}

	if resp.StatusCode != http.StatusOK {
		logrus.Warnf("Image download failed with status %d for %s", resp.StatusCode, src)
		return imgMarkdown
	}

	contentType := resp.Header.Get("Content-Type")
	if strings.Contains(contentType, "text/html") {
		logrus.Warnf("Image URL returned HTML (likely auth redirect): %s", src)
		return imgMarkdown
	}

	imgData, err := io.ReadAll(resp.Body)
	if err != nil {
		logrus.Debugf("Failed to read image data from %s: %v", src, err)
		return imgMarkdown
	}

	respContentType := resp.Header.Get("Content-Type")
	if respContentType == "" {
		respContentType = http.DetectContentType(imgData)
	}

	mimeType := "image/png"
	if strings.HasPrefix(respContentType, "image/") {
		mimeType = respContentType
	}

	base64Data := base64.StdEncoding.EncodeToString(imgData)
	logrus.Debugf("Successfully embedded image: %s (%d bytes, %s)", src, len(imgData), mimeType)

	return fmt.Sprintf("![%s](data:%s;base64,%s)", alt, mimeType, base64Data)
}

// preprocessConfluenceImages converts Confluence-specific image tags to standard img tags
// Confluence uses <ac:image> and <ri:attachment> or <ri:url> tags
func (c *ConfluenceAdapter) preprocessConfluenceImages(htmlContent string) string {
	acImageRegex := regexp.MustCompile(`<ac:image[^>]*>([\s\S]*?)</ac:image>`)

	result := acImageRegex.ReplaceAllStringFunc(htmlContent, func(match string) string {
		alt := ""
		var src string
		srcType := ""

		altRegex := regexp.MustCompile(`ac:alt="([^"]*)"`)
		if altMatches := altRegex.FindStringSubmatch(match); len(altMatches) > 1 {
			alt = altMatches[1]
		}

		attachmentRegex := regexp.MustCompile(`<ri:attachment[^>]*ri:filename="([^"]*)"[^>]*/>`)
		if attachmentMatches := attachmentRegex.FindStringSubmatch(match); len(attachmentMatches) > 1 {
			src = attachmentMatches[1]
			srcType = "attachment"
		}

		urlRegex := regexp.MustCompile(`<ri:url[^>]*ri:value="([^"]*)"[^>]*/>`)
		if urlMatches := urlRegex.FindStringSubmatch(match); len(urlMatches) > 1 {
			src = urlMatches[1]
			srcType = "url"
		}

		if src == "" {
			logrus.Debugf("No image source found in Confluence image tag: %s", match)
			return ""
		}

		logrus.Debugf("Found Confluence image: type=%s, src=%s, alt=%s", srcType, src, alt)

		if srcType == "attachment" {
			imgMarkdown := c.downloadConfluenceAttachment(src, alt)
			return imgMarkdown
		} else if srcType == "url" {
			if !strings.HasPrefix(src, "http://") && !strings.HasPrefix(src, "https://") {
				src = c.config.BaseURL + src
			}
			imgMarkdown := c.downloadAndEmbedImage(src, alt)
			return imgMarkdown
		}

		return fmt.Sprintf("![%s](%s)", alt, src)
	})

	return result
}

// downloadConfluenceAttachment downloads a Confluence attachment and returns embedded markdown
func (c *ConfluenceAdapter) downloadConfluenceAttachment(filename, alt string) string {
	u, _ := url.Parse(c.config.BaseURL)
	cookies := c.cookieJar.Cookies(u)
	if len(cookies) == 0 {
		if err := c.loginAndGetSessionCookies(); err != nil {
			logrus.Warnf("Failed to login for attachment download: %v", err)
		}
	}

	apiURL := fmt.Sprintf("%s/rest/api/content/%s/child/attachment?filename=%s", c.config.BaseURL, c.currentPageID, url.QueryEscape(filename))
	logrus.Debugf("Fetching attachment info from API: %s", apiURL)

	req, err := http.NewRequestWithContext(context.Background(), "GET", apiURL, nil)
	if err != nil {
		logrus.Warnf("Failed to create attachment API request: %v", err)
		return ""
	}

	req.SetBasicAuth(c.config.Username, c.config.APIKey)
	req.Header.Set("Accept", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		logrus.Warnf("Failed to fetch attachment info: %v", err)
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logrus.Warnf("Attachment API returned status %d", resp.StatusCode)
		return ""
	}

	var attachmentResp struct {
		Results []struct {
			ID    string `json:"id"`
			Title string `json:"title"`
			Links struct {
				Download string `json:"download"`
			} `json:"_links"`
		} `json:"results"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&attachmentResp); err != nil {
		logrus.Warnf("Failed to decode attachment response: %v", err)
		return ""
	}

	if len(attachmentResp.Results) == 0 {
		logrus.Warnf("No attachment found with filename: %s", filename)
		return ""
	}

	attachment := attachmentResp.Results[0]
	downloadLink := attachment.Links.Download
	logrus.Debugf("Found attachment ID: %s, Title: %s, Download: %s", attachment.ID, attachment.Title, downloadLink)

	if downloadLink == "" {
		logrus.Warnf("No download link found for attachment: %s", filename)
		return ""
	}

	if !strings.HasPrefix(downloadLink, "http") {
		downloadLink = c.config.BaseURL + downloadLink
	}

	if strings.Contains(downloadLink, "?") {
		downloadLink = downloadLink + "&download=true"
	} else {
		downloadLink = downloadLink + "?download=true"
	}

	logrus.Debugf("Downloading attachment with session cookie: %s", downloadLink)

	return c.downloadAndEmbedImage(downloadLink, alt)
}

// htmlToText converts HTML content to plain text
func (c *ConfluenceAdapter) HtmlToText(htmlContent string) string {
	doc, err := html.Parse(strings.NewReader(htmlContent))
	if err != nil {
		logrus.Warnf("Failed to parse HTML: %v", err)
		return htmlContent
	}

	var text strings.Builder
	c.extractText(doc, &text)
	return strings.TrimSpace(text.String())
}

// extractText recursively extracts text from HTML nodes
func (c *ConfluenceAdapter) extractText(n *html.Node, text *strings.Builder) {
	if n.Type == html.TextNode {
		text.WriteString(n.Data)
		return
	}

	// Handle special elements
	if n.Type == html.ElementNode {
		switch n.Data {
		case "br":
			text.WriteString("\n")
			return
		case "p":
			// Add line break before paragraph (except if it's the first element)
			if text.Len() > 0 && !strings.HasSuffix(text.String(), "\n") {
				text.WriteString("\n")
			}
			// Process children
			for child := n.FirstChild; child != nil; child = child.NextSibling {
				c.extractText(child, text)
			}
			// Add double line break after paragraph
			text.WriteString("\n\n")
			return
		case "div", "h1", "h2", "h3", "h4", "h5", "h6":
			// Add line break before other block elements (except if it's the first element)
			if text.Len() > 0 && !strings.HasSuffix(text.String(), "\n") {
				text.WriteString("\n")
			}
			// Process children
			for child := n.FirstChild; child != nil; child = child.NextSibling {
				c.extractText(child, text)
			}
			// Add single line break after other block elements
			text.WriteString("\n")
			return
		}
	}

	// Process children for other elements
	for child := n.FirstChild; child != nil; child = child.NextSibling {
		c.extractText(child, text)
	}
}

// sanitizeFilename converts a title to a safe filename
func (c *ConfluenceAdapter) SanitizeFilename(title string) string {
	filename := strings.ToLower(strings.TrimSpace(title))

	// Keep unicode letters/numbers so non-English page titles are preserved.
	reg := regexp.MustCompile(`[^\p{L}\p{N}\s_.-]`)
	filename = reg.ReplaceAllString(filename, "_")

	reg = regexp.MustCompile(`[\s_]+`)
	filename = reg.ReplaceAllString(filename, "_")

	filename = strings.Trim(filename, "_")

	if len(filename) > 100 {
		filename = filename[:100]
	}

	if filename == "" {
		filename = "page"
	}

	return filename
}

// generateUniqueFilename generates a unique filename using the (possibly hierarchical) title
func (c *ConfluenceAdapter) generateUniqueFilename(title, pageID string, useMarkdown bool) string {
	sanitizedTitle := c.SanitizeFilename(title)

	ext := ".txt"
	if useMarkdown {
		ext = ".md"
	}

	return fmt.Sprintf("%s%s", sanitizedTitle, ext)
}

// buildHierarchicalTitles builds a map of page ID -> full hierarchical title
// e.g. "Parent / Child / Grandchild"
func (c *ConfluenceAdapter) buildHierarchicalTitles(pages []ConfluencePage) map[string]string {
	idToPage := make(map[string]ConfluencePage, len(pages))
	for _, p := range pages {
		idToPage[p.ID] = p
	}

	cache := make(map[string]string, len(pages))

	var build func(p ConfluencePage) string
	build = func(p ConfluencePage) string {
		if v, ok := cache[p.ID]; ok {
			return v
		}

		titles := []string{p.Title}
		current := p
		for {
			parentID := current.ParentID
			if parentID == "" {
				break
			}
			parent, ok := idToPage[parentID]
			if !ok {
				break
			}
			// Avoid repeating the same title when parent/child share identical names
			if len(titles) == 0 || parent.Title != titles[0] {
				titles = append([]string{parent.Title}, titles...)
			}
			current = parent
		}

		full := strings.Join(titles, " / ")
		cache[p.ID] = full
		return full
	}

	for _, p := range pages {
		build(p)
	}

	return cache
}

// GetLastSync returns the last sync time
func (c *ConfluenceAdapter) GetLastSync() time.Time {
	return c.lastSync
}

// SetLastSync sets the last sync time
func (c *ConfluenceAdapter) SetLastSync(t time.Time) {
	c.lastSync = t
}

// fetchUsersByIds fetches user information for multiple account IDs using the bulk API
func (c *ConfluenceAdapter) fetchUsersByIds(ctx context.Context, accountIds []string) (map[string]*ConfluenceUser, error) {
	// Create the request body
	requestBody := map[string]interface{}{
		"accountIds": accountIds,
	}

	// Marshal the request body
	body, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	// Prepare the URL for the bulk user lookup endpoint
	url := fmt.Sprintf("%s/api/v2/users-bulk", c.config.BaseURL)

	// Create the request
	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(string(body)))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set authentication and headers
	req.SetBasicAuth(c.config.Username, c.config.APIKey)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	logrus.Debugf("Confluence bulk user API URL: %s", url)
	logrus.Debugf("Confluence bulk user request body: %s", string(body))

	// Make the request
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body) // Consume body for proper connection reuse
		logrus.Errorf("Confluence bulk user API failed - Status: %d, URL: %s, Response: %s", resp.StatusCode, url, string(body))
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse the response
	var userResponse struct {
		Results []ConfluenceUser `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&userResponse); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// Create a map of account IDs to users for easy lookup
	userMap := make(map[string]*ConfluenceUser)
	for i := range userResponse.Results {
		user := &userResponse.Results[i]
		userMap[user.AccountID] = user
	}

	return userMap, nil
}
