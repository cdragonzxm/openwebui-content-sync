package adapter

import (
	"reflect"
	"strings"
	"testing"
)

func TestStripLeafIfAncestorsPrefixed(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{
			name: "leaf embeds full ancestor path",
			in:   []string{"mdk对外文档", "ax520c", "媒体处理软件开发参考", "mdk对外文档 / ax520c / 媒体处理软件开发参考 / 系统概述"},
			want: []string{"mdk对外文档", "ax520c", "媒体处理软件开发参考", "系统概述"},
		},
		{
			name: "no redundant prefix",
			in:   []string{"mdk对外文档", "ax520c", "系统概述"},
			want: []string{"mdk对外文档", "ax520c", "系统概述"},
		},
		{
			name: "single segment",
			in:   []string{"only"},
			want: []string{"only"},
		},
		{
			name: "strip once when leaf repeats ancestors",
			in:   []string{"A", "B", "A / B / C / D"},
			want: []string{"A", "B", "C / D"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stripLeafIfAncestorsPrefixed(tt.in)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("stripLeafIfAncestorsPrefixed() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCollapseConsecutiveDuplicateSegments(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{
			name: "mdk ax520c duplicate pairs",
			in:   []string{"mdk对外文档", "mdk对外文档", "ax520c", "ax520c", "媒体处理软件开发参考", "系统控制"},
			want: []string{"mdk对外文档", "ax520c", "媒体处理软件开发参考", "系统控制"},
		},
		{
			name: "no adjacent dup",
			in:   []string{"a", "b", "c"},
			want: []string{"a", "b", "c"},
		},
		{
			name: "triple repeat",
			in:   []string{"x", "x", "x", "y"},
			want: []string{"x", "y"},
		},
		{
			name: "case-insensitive consecutive",
			in:   []string{"PISP-V1", "pisp-v1", "系统控制"},
			want: []string{"PISP-V1", "系统控制"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := collapseConsecutiveDuplicateSegments(tt.in)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("collapseConsecutiveDuplicateSegments() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCollapseDuplicateHierarchyPrefix(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{
			name: "duplicate first two levels once",
			in:   []string{"mdk对外文档", "mdk对外文档", "ax520c", "ax520c", "媒体处理软件开发参考", "系统概述"},
			want: []string{"mdk对外文档", "ax520c", "媒体处理软件开发参考", "系统概述"},
		},
		{
			name: "only leading aabb is collapsed second block unchanged",
			in:   []string{"a", "a", "b", "b", "a", "a", "b", "b", "c"},
			want: []string{"a", "b", "a", "a", "b", "b", "c"},
		},
		{
			name: "no collapse when different",
			in:   []string{"a", "b", "c", "d"},
			want: []string{"a", "b", "c", "d"},
		},
		{
			name: "short slice unchanged",
			in:   []string{"a", "b", "c"},
			want: []string{"a", "b", "c"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := collapseDuplicateHierarchyPrefix(tt.in)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("collapseDuplicateHierarchyPrefix() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSplitTitleIntoPathSegments_SlashWithoutSpaces(t *testing.T) {
	got := splitTitleIntoPathSegments("ax520c/媒体处理软件开发参考/概述")
	if len(got) != 3 || got[0] != "ax520c" || got[2] != "概述" {
		t.Fatalf("splitTitleIntoPathSegments() = %v", got)
	}
}

func TestDedupeChildTokensAgainstParentTokens(t *testing.T) {
	if got := dedupeChildTokensAgainstParentTokens("ax520c", "ax520c_媒体处理软件开发参考"); got != "媒体处理软件开发参考" {
		t.Errorf("got %q", got)
	}
	if got := dedupeChildTokensAgainstParentTokens("packer_配置", "packer_安装"); got != "安装" {
		t.Errorf("got %q", got)
	}
}

func TestBuildHierarchicalTitles_OnlyImmediateParent(t *testing.T) {
	c := &ConfluenceAdapter{}
	pages := []ConfluencePage{
		{ID: "p1", Title: "mdk对外文档", ParentID: ""},
		{ID: "p2", Title: "mdk对外文档", ParentID: "p1"},
		{ID: "p3", Title: "ax520c", ParentID: "p2"},
		{ID: "p4", Title: "ax520c", ParentID: "p3"},
		{ID: "p5", Title: "媒体处理软件开发参考", ParentID: "p4"},
		{ID: "p6", Title: "系统控制", ParentID: "p5"},
	}
	m := c.buildHierarchicalTitles(pages)
	want := "媒体处理软件开发参考 / 系统控制"
	if m["p6"] != want {
		t.Errorf("buildHierarchicalTitles(p6) = %q, want %q", m["p6"], want)
	}
}

func TestBuildHierarchicalTitles_LeafEmbedsUnderscorePathWithParents(t *testing.T) {
	c := &ConfluenceAdapter{}
	pages := []ConfluencePage{
		{ID: "p1", Title: "mdk对外文档", ParentID: ""},
		{ID: "p2", Title: "ax520c_媒体处理软件开发参考_图像信号处理_pisp-v1_pisp-v1_概述", ParentID: "p1"},
	}
	m := c.buildHierarchicalTitles(pages)
	// Child normalizes to last segment "概述"; parent "mdk对外文档"
	want := "mdk对外文档 / 概述"
	if m["p2"] != want {
		t.Errorf("buildHierarchicalTitles(p2) = %q, want %q", m["p2"], want)
	}
}

func TestSplitTitleIntoPathSegments_UnderscoreOnlyBreadcrumb(t *testing.T) {
	in := "mdk对外文档_mdk对外文档_ax520c_ax520c_媒体处理软件开发参考"
	got := splitTitleIntoPathSegments(in)
	if len(got) < 3 {
		t.Fatalf("expected multiple segments from underscore path, got %v", got)
	}
	c := &ConfluenceAdapter{}
	stem := c.finalizeFilenameStemFromTitle(in)
	underscores := strings.Count(stem, "_")
	if underscores > 1 {
		t.Errorf("filename stem should have at most one underscore (parent_child), got %q (%d underscores)", stem, underscores)
	}
}

func TestBuildHierarchicalTitles_UnderscoreOnlySingleTitle(t *testing.T) {
	c := &ConfluenceAdapter{}
	pages := []ConfluencePage{
		{ID: "leaf", Title: "ax520c_媒体处理软件开发参考_图像信号处理_pisp-v1_pisp-v1_系统控制", ParentID: ""},
	}
	m := c.buildHierarchicalTitles(pages)
	want := "系统控制"
	if m["leaf"] != want {
		t.Errorf("buildHierarchicalTitles(leaf) = %q, want %q", m["leaf"], want)
	}
}

func TestTrimBreadcrumbToMaxLevels(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		max  int
		want []string
	}{
		{name: "keep last three of five", in: []string{"a", "b", "c", "d", "e"}, max: 3, want: []string{"c", "d", "e"}},
		{name: "unchanged when at most three", in: []string{"a", "b", "c"}, max: 3, want: []string{"a", "b", "c"}},
		{name: "single segment", in: []string{"only"}, max: 3, want: []string{"only"}},
		{name: "max zero leaves slice", in: []string{"a", "b"}, max: 0, want: []string{"a", "b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := trimBreadcrumbToMaxLevels(tt.in, tt.max)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("trimBreadcrumbToMaxLevels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuildHierarchicalTitles_BreadcrumbInLeafTitle(t *testing.T) {
	c := &ConfluenceAdapter{}
	pages := []ConfluencePage{
		{ID: "p1", Title: "mdk对外文档", ParentID: ""},
		{ID: "p2", Title: "ax520c", ParentID: "p1"},
		{ID: "p3", Title: "媒体处理软件开发参考", ParentID: "p2"},
		{ID: "p4", Title: "mdk对外文档 / ax520c / 媒体处理软件开发参考 / 系统概述", ParentID: "p3"},
	}
	m := c.buildHierarchicalTitles(pages)
	want := "媒体处理软件开发参考 / 系统概述"
	if m["p4"] != want {
		t.Errorf("buildHierarchicalTitles(p4) = %q, want %q", m["p4"], want)
	}
}

func TestBuildHierarchicalTitles_SingleTitleWithDuplicateBreadcrumb(t *testing.T) {
	c := &ConfluenceAdapter{}
	pages := []ConfluencePage{
		{ID: "leaf", Title: "mdk对外文档 / mdk对外文档 / ax520c / ax520c / 媒体处理软件开发参考 / 系统控制", ParentID: ""},
	}
	m := c.buildHierarchicalTitles(pages)
	want := "系统控制"
	if m["leaf"] != want {
		t.Errorf("buildHierarchicalTitles(leaf) = %q, want %q", m["leaf"], want)
	}
}

func TestBuildHierarchicalTitles_CaseInsensitiveParentChildTitle(t *testing.T) {
	c := &ConfluenceAdapter{}
	pages := []ConfluencePage{
		{ID: "p1", Title: "PISP-V1", ParentID: ""},
		{ID: "p2", Title: "pisp-v1", ParentID: "p1"},
		{ID: "p3", Title: "概述", ParentID: "p2"},
	}
	m := c.buildHierarchicalTitles(pages)
	want := "pisp-v1 / 概述"
	if m["p3"] != want {
		t.Errorf("buildHierarchicalTitles(p3) = %q, want %q", m["p3"], want)
	}
}

func TestBuildHierarchicalTitles_ParentPageModeChain(t *testing.T) {
	c := &ConfluenceAdapter{}
	pages := []ConfluencePage{
		{ID: "root", Title: "根页面", ParentID: ""},
		{ID: "c1", Title: "子页", ParentID: "root"},
	}
	m := c.buildHierarchicalTitles(pages)
	if m["c1"] != "根页面 / 子页" {
		t.Errorf("buildHierarchicalTitles(c1) = %q", m["c1"])
	}
	if m["root"] != "根页面" {
		t.Errorf("buildHierarchicalTitles(root) = %q", m["root"])
	}
}
