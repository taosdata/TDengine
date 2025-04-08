/*
 *  CUnit - A Unit testing framework library for C.
 *  Copyright (C) 2006    Jerry St.Clair
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Library General Public
 *  License as published by the Free Software Foundation; either
 *  version 2 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Library General Public License for more details.
 *
 *  You should have received a copy of the GNU Library General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

/*
 *  Implementation of the wxWidgets Test Interface.
 *
 *  May 2006      Initial implementation (JDS)
 */

/** @file
 *  wxWidgets gui test interface (implementation).
 */
/** @addtogroup wxWidgets
 @{
*/

// For compilers that support precompilation, includes "wx.h".
#include <wx/wxprec.h>

#ifdef __BORLANDC__
#pragma hdrstop
#endif

#ifndef WX_PRECOMP
// Include your minimal set of headers here, or wx.h
#include <wx/wx.h>
#endif
#include <wx/image.h>
#include <wx/imaglist.h>
#include <wx/colour.h>
#include <wx/treectrl.h>
#include <wx/notebook.h>

#include "icon_suite_active.xpm"
#include "icon_suite_active_open.xpm"
#include "icon_suite_inactive.xpm"
#include "icon_suite_inactive_open.xpm"
#include "icon_test_active.xpm"
#include "icon_test_inactive.xpm"
#include "icon_test_active_suite_inactive.xpm"
#include "icon_test_inactive_suite_inactive.xpm"

#include "CUnit.h"
#include "TestDB.h"
#include "Util.h"
#include "TestRun.h"
#include "MyMem.h"
#include "wxWidget.h"
#include "CUnit_intl.h"   // include after wx.h to customize gettext macros set in wx.h


//* New application type for the CUnit wxWidgets interface.
class CUnitApp: public wxApp
{
public:
    virtual bool OnInit();    //< Initializes the application.
};

//* Main window for the CUnit wxWidgets interface
class TopFrame: public wxFrame
{
public:
    TopFrame(const wxString& title, const wxPoint& pos, const wxSize& size);
    void SetActiveTopFrame(TopFrame *frame);
    
    enum {
        TreeCtrlIcon_SuiteActive,
        TreeCtrlIcon_SuiteActiveOpen,
        TreeCtrlIcon_SuiteInactive,
        TreeCtrlIcon_SuiteInactiveOpen,
        TreeCtrlIcon_TestActive,
        TreeCtrlIcon_TestInactive,
        TreeCtrlIcon_TestActiveSuiteInactive,
        TreeCtrlIcon_TestInactiveSuiteInactive,
    };
            
    // callback functions for running tests
    static void SuiteStartMessageHandler(const CU_pSuite pSuite);
    static void TestStartMessageHandler(const CU_pTest pTest, const CU_pSuite pSuite);
    static void TestCompleteMessageHandler(const CU_pTest pTest, 
                                           const CU_pSuite pSuite,
                                           const CU_pFailureRecord pFailure);
    static void SuiteCompleteMessageHandler(const CU_pSuite pSuite,
                                            const CU_pFailureRecord pFailure);

protected:
    void ClearPreviousStats(void);
    wxTreeItemId FindSuite(CU_pSuite pSuite);
    wxTreeItemId FindTest(CU_pSuite pSuite, CU_pTest pTest);
    void ActivateSuiteInfo(void);
    void DeactivateSuiteInfo(void);
    void ActivateTestInfo(void);
    void DeactivateTestInfo(void);
    void ActivateRunResultInfo(void);
    void DeactivateRunResultInfo(void);
    void WriteFailureRecords(wxTextCtrl *textctrl, 
                             unsigned int nFailures, 
                             CU_pFailureRecord pFailure,
                             bool reset = false);
    
    // event procedures
    void OnQuit(wxCommandEvent& event);
    void OnAbout(wxCommandEvent& event);
    void OnTreeSelectionChanged(wxTreeEvent &event);
    void OnTreeExpandAll(wxCommandEvent& event);
    void OnTreeCollapseAll(wxCommandEvent& event);
    void OnActivateSelected(wxCommandEvent& event);
    void OnDeactivateSelected(wxCommandEvent& event);
    void OnActivateAllSuites(wxCommandEvent& event);
    void OnDeactivateAllSuites(wxCommandEvent& event);
    void OnActivateAll(wxCommandEvent& event);
    void OnDeactivateAll(wxCommandEvent& event);
    void OnOptionColorizeTreeItems(wxCommandEvent& event);
    void OnOptionFailOnInactive(wxCommandEvent& event);
    void OnRunAll(wxCommandEvent& event);
    void OnCloseWindow(wxCloseEvent& event);
    void OnSuiteActiveCheckbox(wxCommandEvent& event);
    void OnTestActiveCheckbox(wxCommandEvent& event);

    void OnTreeSelChanged(wxTreeEvent& event);

private:
    // controls needing persistent hooks
    wxTreeCtrl   *m_treectrl;             //< wxTreeCtrl holding suite & test references.
    wxStaticBox  *m_suite_info_box;
    wxStaticText *m_suite_name_label;
    wxTextCtrl   *m_suite_name_text;
    wxStaticText *m_suite_init_label;
    wxStaticText *m_suite_init_text;
    wxStaticText *m_suite_clean_label;
    wxStaticText *m_suite_clean_text;
    wxCheckBox   *m_suite_active_box;
    wxStaticBox  *m_test_info_box;
    wxStaticText *m_test_name_label;
    wxTextCtrl   *m_test_name_text;
    wxCheckBox   *m_test_active_box;
    wxStaticBox  *m_result_box;
    wxStaticText *m_result_ntests_label;
    wxTextCtrl   *m_result_ntests_text;
    wxStaticText *m_result_nasserts_label;
    wxTextCtrl   *m_result_nasserts_text;
    wxStaticText *m_result_nfailures_label;
    wxTextCtrl   *m_result_nfailures_text;
    wxStaticBox  *m_failure_listing_box;
    wxTextCtrl   *m_failure_listing_text;    
    wxSizer      *m_page2sizer;
    wxStaticText *m_run_summary_text;
    wxTextCtrl   *m_all_failure_listing_text;
    
    bool m_suite_info_active;
    bool m_test_info_active;
    bool m_result_info_active;
    bool m_failure_info_active;

    static TopFrame *m_ActiveTopFrame;  //< The active TopFrame, used in test run event handlers.
    CU_RunSummary m_RunSummary_suite;   //< For storing intermediate test run results.
    CU_RunSummary m_RunSummary_test;    //< For storing intermediate test run results.
    bool m_option_colorize_tree_items;  //< Flag for menu option.
    bool m_option_fail_on_inactive;     //< Flag for menu option.
    bool m_test_run_complete;           //< Flag for whether a test run has been completed
    
    DECLARE_EVENT_TABLE()
};

IMPLEMENT_APP(CUnitApp)

enum
{
    menu_FileExit = 1,
    menu_ViewExpandAll,
    menu_ViewCollapseAll,
    menu_ViewColorizeTreeItems,
    menu_OptionActivate,
    menu_OptionActivateSelected,
    menu_OptionActivateAllSuites,
    menu_OptionActivateAll,
    menu_OptionDeactivate,
    menu_OptionDeactivateSelected,
    menu_OptionDeactivateAllSuites,
    menu_OptionDeactivateAll,
    menu_OptionFailOnInactive,
    menu_RunRunAll,
    menu_HelpAbout,
    
    checkbox_SuiteActive,
    checkbox_TestActive,
    tree_Listing,
};

BEGIN_EVENT_TABLE(TopFrame, wxFrame)
  EVT_MENU(menu_FileExit,                   TopFrame::OnQuit)
  EVT_MENU(menu_ViewExpandAll,              TopFrame::OnTreeExpandAll)
  EVT_MENU(menu_ViewCollapseAll,            TopFrame::OnTreeCollapseAll)
  EVT_MENU(menu_OptionActivateSelected,     TopFrame::OnActivateSelected)
  EVT_MENU(menu_OptionActivateAllSuites,    TopFrame::OnActivateAllSuites)
  EVT_MENU(menu_OptionActivateAll,          TopFrame::OnActivateAll)
  EVT_MENU(menu_OptionDeactivateSelected,   TopFrame::OnDeactivateSelected)
  EVT_MENU(menu_OptionDeactivateAllSuites,  TopFrame::OnDeactivateAllSuites)
  EVT_MENU(menu_OptionDeactivateAll,        TopFrame::OnDeactivateAll)
  EVT_MENU(menu_ViewColorizeTreeItems,    TopFrame::OnOptionColorizeTreeItems)
  EVT_MENU(menu_OptionFailOnInactive,       TopFrame::OnOptionFailOnInactive)
  EVT_MENU(menu_RunRunAll,                  TopFrame::OnRunAll)
  EVT_MENU(menu_HelpAbout,                  TopFrame::OnAbout)
  
  EVT_CLOSE(                                TopFrame::OnCloseWindow)
  EVT_CHECKBOX(checkbox_SuiteActive,        TopFrame::OnSuiteActiveCheckbox)
  EVT_CHECKBOX(checkbox_TestActive,         TopFrame::OnTestActiveCheckbox)
  EVT_TREE_SEL_CHANGED(tree_Listing,        TopFrame::OnTreeSelectionChanged) 
END_EVENT_TABLE()

// wxTreeItemData descendent to hold a run data for a CU_pSuite or CU_pTest
// The pointer is only held and is never deleted by this class.
// This is just a data container, so make all members public.
class TreeCUnitRunData : public wxTreeItemData
{
public:
    TreeCUnitRunData(void)
    : nAsserts(0), nFailures(0), pFirstFailure(NULL) {}

    unsigned long     nAsserts;
    unsigned long     nFailures;
    CU_pFailureRecord pFirstFailure;
};

// wxTreeItemData descendent to hold a CU_pSuite pointer
// The pointer is only held and is never deleted by this class.
// This is just a data container, so make all members public.
class TreeSuiteData : public TreeCUnitRunData
{
public:
    TreeSuiteData(CU_pSuite pSuite)
    : pSuite(pSuite), nTestsRun(0) {}

    CU_pSuite         pSuite;
    unsigned long     nTestsRun;
};

// wxTreeItemData descendent to hold a CU_pTest pointer
// The pointer is only held and is never deleted by this class.
// This is just a data container, so make all members public.
class TreeTestData : public TreeCUnitRunData
{
public:
    TreeTestData(CU_pTest pTest)
    : pTest(pTest) {}

    CU_pTest          pTest;
};

// Test whether an item is a suite (only has parent, not grandparent)
static bool isSuite(wxTreeCtrl *tree, wxTreeItemId &item)
{
  bool result = false;
  wxTreeItemId parent = tree->GetItemParent(item);
  if ((true == parent.IsOk()) &&
      (false == tree->GetItemParent(parent).IsOk())) {
    result = true;
  }
  return result;
}

// Test whether an item is a test (has both parent and grandparent)
static bool isTest(wxTreeCtrl *tree, wxTreeItemId &item)
{
  bool result = false;
  wxTreeItemId parent = tree->GetItemParent(item);
  if ((true == parent.IsOk()) &&
      (true == tree->GetItemParent(parent).IsOk())) {
    result = true;
  }
  return result;
}

// Format a suite tree item
static void FormatTreeItem_Suite(wxTreeCtrl *tree, wxTreeItemId &item, bool isActive)
{
  assert(true == isSuite(tree, item));
  if (true == isActive) {
    tree->SetItemTextColour(item, wxColour("BLACK"));
    tree->SetItemImage(item, 
                       ((true == tree->IsExpanded(item)) 
                            ? TopFrame::TreeCtrlIcon_SuiteActiveOpen
                            : TopFrame::TreeCtrlIcon_SuiteActive),
                       wxTreeItemIcon_Normal);
  }
  else {
    tree->SetItemTextColour(item, wxColour("FIREBRICK"));
    tree->SetItemImage(item, 
                       ((true == tree->IsExpanded(item)) 
                            ? TopFrame::TreeCtrlIcon_SuiteInactiveOpen
                            : TopFrame::TreeCtrlIcon_SuiteInactive),
                       wxTreeItemIcon_Normal);
  }
}

// Format a test tree item
static void FormatTreeItem_Test(wxTreeCtrl *tree, 
                                wxTreeItemId &item, 
                                bool isActiveTest,
                                bool isActiveSuite)
{
  if (true == isActiveTest) {
    tree->SetItemTextColour(item, wxColour("BLACK"));
    tree->SetItemImage(item, 
                       ((true == isActiveSuite) 
                              ? TopFrame::TreeCtrlIcon_TestActive
                              : TopFrame::TreeCtrlIcon_TestActiveSuiteInactive),
                       wxTreeItemIcon_Normal);
  }
  else {
    tree->SetItemTextColour(item, wxColour("FIREBRICK"));
    tree->SetItemImage(item, 
                       ((true == isActiveSuite) 
                              ? TopFrame::TreeCtrlIcon_TestInactive
                              : TopFrame::TreeCtrlIcon_TestInactiveSuiteInactive),
                       wxTreeItemIcon_Normal);
  }
}

//* Base class for functors operating on a wxTreeCtrl element
struct TreeItemFunctorBase
{
  //* Function returning true on success, false on failure
  virtual bool func(wxTreeCtrl *WXUNUSED(tree), wxTreeItemId &WXUNUSED(item)) = 0;
};

//* Apply a functor to each suite in a wxTreeCtrl
static void ForEachSuite(wxTreeCtrl *tree, TreeItemFunctorBase &functor)
{
  assert(NULL != tree);

  wxTreeItemId item = tree->GetRootItem();
  wxTreeItemIdValue cookie;
  item = tree->GetFirstChild(item, cookie);
  while (true == item.IsOk()) {
    functor.func(tree, item);
    item = tree->GetNextSibling(item);
  }
}

//* Apply a functor to each test in the specified suite item in a wxTreeCtrl
static void ForEachTestInSuite(wxTreeCtrl *tree, 
                               wxTreeItemId &suite_item,
                               TreeItemFunctorBase &test_functor)
{
  assert(NULL != tree);
  if (true == isSuite(tree, suite_item)) {
    wxTreeItemIdValue cookie;
    wxTreeItemId test_item = tree->GetFirstChild(suite_item, cookie);
    while (true == test_item.IsOk()) {
      test_functor.func(tree, test_item);
      test_item = tree->GetNextChild(suite_item, cookie);
    }
  }
}

//* Apply a functor to each item in a wxTreeCtrl
static void ForEachSuiteAndTest(wxTreeCtrl *tree, 
                                TreeItemFunctorBase &suite_functor,
                                TreeItemFunctorBase &test_functor)
{
  assert(NULL != tree);

  wxTreeItemId suite_item = tree->GetRootItem();  // note - not a suite yet, but will be
  wxTreeItemIdValue cookie;
  suite_item = tree->GetFirstChild(suite_item, cookie);
  while (true == suite_item.IsOk()) {
    suite_functor.func(tree, suite_item);
    wxTreeItemId test_item = tree->GetFirstChild(suite_item, cookie);
    while (true == test_item.IsOk()) {
      test_functor.func(tree, test_item);
      test_item = tree->GetNextChild(suite_item, cookie);
    }
    suite_item = tree->GetNextSibling(suite_item);
  }
}

// Tree item functor - expands node
struct TreeItemFunctor_Expand : public TreeItemFunctorBase
{
  virtual bool func(wxTreeCtrl *tree, wxTreeItemId &item)
  { 
    assert(NULL != tree);
    tree->Expand(item);
    return true;
  }
};
static TreeItemFunctor_Expand f_expand_item_functor;

// Tree item functor - collapses node
struct TreeItemFunctor_Collapse : public TreeItemFunctorBase
{
  virtual bool func(wxTreeCtrl *tree, wxTreeItemId &item)
  { 
    assert(NULL != tree);
    tree->Collapse(item);
    return true;
  }
};
static TreeItemFunctor_Collapse f_collapse_item_functor;

// Tree item functor - activates a node if it is a suite
struct TreeItemFunctor_ActivateSuite : public TreeItemFunctorBase
{
  virtual bool func(wxTreeCtrl *tree, wxTreeItemId &item)
  { 
    TreeSuiteData *data = dynamic_cast<TreeSuiteData*>(tree->GetItemData(item));
    if (NULL != data) {
      // activate the suite
      data->pSuite->fActive = CU_TRUE;
      // format the suite item in the tree
      FormatTreeItem_Suite(tree, item, true);

      // format each test item in the suite
      wxTreeItemIdValue cookie;
      wxTreeItemId test_item = tree->GetFirstChild(item, cookie);
      while (true == test_item.IsOk()) {
        TreeTestData *data = dynamic_cast<TreeTestData*>(tree->GetItemData(test_item));
        FormatTreeItem_Test(tree, test_item, 
                            ((NULL != data) ? (CU_TRUE == data->pTest->fActive) : true),
                            true);
        test_item = tree->GetNextChild(item, cookie);
      }

      return true;
    }
    else {
      return false;
    }
  }
};
static TreeItemFunctor_ActivateSuite f_activate_suite_item_functor;

// Tree item functor - deactivates a node if it is a suite
struct TreeItemFunctor_DeactivateSuite : public TreeItemFunctorBase
{
  virtual bool func(wxTreeCtrl *tree, wxTreeItemId &item)
  { 
    TreeSuiteData *data = dynamic_cast<TreeSuiteData*>(tree->GetItemData(item));
    if (NULL != data) {
      // deactivate the suite
      data->pSuite->fActive = CU_FALSE;
      // format the suite item in the tree
      FormatTreeItem_Suite(tree, item, false);

      // format each test item in the suite
      wxTreeItemIdValue cookie;
      wxTreeItemId test_item = tree->GetFirstChild(item, cookie);
      while (true == test_item.IsOk()) {
        TreeTestData *data = dynamic_cast<TreeTestData*>(tree->GetItemData(test_item));
        FormatTreeItem_Test(tree, test_item, 
                            ((NULL != data) ? (CU_TRUE == data->pTest->fActive) : true),
                            false);
        test_item = tree->GetNextChild(item, cookie);
      }

      return true;
    }
    else {
      return false;
    }
  }
};
static TreeItemFunctor_DeactivateSuite f_deactivate_suite_item_functor;

// Tree item functor - activates a node if it is a test
struct TreeItemFunctor_ActivateTest : public TreeItemFunctorBase
{
  virtual bool func(wxTreeCtrl *tree, wxTreeItemId &item)
  { 
    TreeTestData *data = dynamic_cast<TreeTestData*>(tree->GetItemData(item));
    if (NULL != data) {
      // activate the test
      data->pTest->fActive = CU_TRUE;
      // format the test item in the tree
      TreeSuiteData *suitedata = dynamic_cast<TreeSuiteData*>(tree->GetItemData(tree->GetItemParent(item)));
      FormatTreeItem_Test(tree, item, true,
                          ((NULL != suitedata) ? (CU_TRUE == suitedata->pSuite->fActive) : true));
      return true;
    }
    else {
      return false;
    }
  }
};
static TreeItemFunctor_ActivateTest f_activate_test_item_functor;

// Tree item functor - deactivates a node if it is a test
struct TreeItemFunctor_DeactivateTest : public TreeItemFunctorBase
{
  virtual bool func(wxTreeCtrl *tree, wxTreeItemId &item)
  { 
    TreeTestData *data = dynamic_cast<TreeTestData*>(tree->GetItemData(item));
    if (NULL != data) {
      // deactivate the test
      data->pTest->fActive = CU_FALSE;
      // format the test item in the tree
      TreeSuiteData *suitedata = dynamic_cast<TreeSuiteData*>(tree->GetItemData(tree->GetItemParent(item)));
      FormatTreeItem_Test(tree, item, false,
                          ((NULL != suitedata) ? (CU_TRUE == suitedata->pSuite->fActive) : true));
      return true;
    }
    else {
      return false;
    }
  }
};
static TreeItemFunctor_DeactivateTest f_deactivate_test_item_functor;

// Tree item functor - clears the run stats for a node if it is a suite
struct TreeItemFunctor_ClearSuiteStats : public TreeItemFunctorBase
{
  virtual bool func(wxTreeCtrl *tree, wxTreeItemId &item)
  { 
    TreeSuiteData *data = dynamic_cast<TreeSuiteData*>(tree->GetItemData(item));
    if (NULL != data) {
      data->nTestsRun = 0;
      data->nAsserts = 0;
      data->nFailures = 0;
      data->pFirstFailure = NULL;
      return true;
    }
    else {
      return false;
    }
  }
};
static TreeItemFunctor_ClearSuiteStats f_clear_suite_stats_functor;

// Tree item functor - clears the run stats for a node if it is a test
struct TreeItemFunctor_ClearTestStats : public TreeItemFunctorBase
{
  virtual bool func(wxTreeCtrl *tree, wxTreeItemId &item)
  { 
    TreeTestData *data = dynamic_cast<TreeTestData*>(tree->GetItemData(item));
    if (NULL != data) {
      data->nAsserts = 0;
      data->nFailures = 0;
      data->pFirstFailure = NULL;
      return true;
    }
    else {
      return false;
    }
  }
};
static TreeItemFunctor_ClearTestStats f_clear_test_stats_functor;

// Tree item functor - colorize the background color based on failure status
struct TreeItemFunctor_ColorizeItemBackground : public TreeItemFunctorBase
{
  virtual bool func(wxTreeCtrl *tree, wxTreeItemId &item)
  { 
    TreeCUnitRunData *data = dynamic_cast<TreeCUnitRunData*>(tree->GetItemData(item));
    assert(NULL != data);
    tree->SetItemBackgroundColour(item, wxColour((data->nFailures > 0) ? "RED" : "GREEN"));
    return true;
  }
};
static TreeItemFunctor_ColorizeItemBackground f_colorize_item_background;

// Tree item functor - resets the background color to white
struct TreeItemFunctor_ResetItemBackgroundColor : public TreeItemFunctorBase
{
  virtual bool func(wxTreeCtrl *tree, wxTreeItemId &item)
  { 
    tree->SetItemBackgroundColour(item, wxColour("WHITE"));
    return true;
  }
};
static TreeItemFunctor_ResetItemBackgroundColor f_reset_item_background_color;

bool CUnitApp::OnInit()
{
    TopFrame *frame = new TopFrame(_("CUnit Testing - wxWidgets Interface"), 
                                   wxPoint(50,50), 
                                   wxSize(450,340));
    frame->CenterOnScreen();
    frame->Show(true);
    SetTopWindow(frame);
    return true;
}

TopFrame *TopFrame::m_ActiveTopFrame = NULL;

void TopFrame::ClearPreviousStats(void)
{
  CU_RunSummary initialized_summary = {0,0,0,0,0,0,0,0,0};
  
  ForEachSuiteAndTest(m_treectrl, f_clear_suite_stats_functor, f_clear_test_stats_functor);
  if (true == m_option_colorize_tree_items) {
    ForEachSuiteAndTest(m_treectrl, f_reset_item_background_color, f_reset_item_background_color);
  }
  m_RunSummary_suite = initialized_summary;
  m_RunSummary_test = initialized_summary;
}

void TopFrame::SetActiveTopFrame(TopFrame *frame)
{
  m_ActiveTopFrame = frame;
}

wxTreeItemId TopFrame::FindSuite(CU_pSuite pSuite)
{
  TreeSuiteData *data;
  wxTreeItemIdValue cookie;
  wxTreeItemId suite_item;

  suite_item = m_treectrl->GetFirstChild(m_treectrl->GetRootItem(), cookie);
  while (true == suite_item.IsOk()) {
    data = dynamic_cast<TreeSuiteData*>(m_treectrl->GetItemData(suite_item));
    if (data->pSuite == pSuite) {
      break;
    }
    suite_item = m_treectrl->GetNextSibling(suite_item);
  }
  return suite_item;
}

wxTreeItemId TopFrame::FindTest(CU_pSuite pSuite, CU_pTest pTest)
{
  TreeTestData *data;
  wxTreeItemIdValue cookie;
  wxTreeItemId suite_item;
  wxTreeItemId test_item;

  suite_item = FindSuite(pSuite);
  if (!suite_item.IsOk()) {
    return suite_item;
  }
  
  test_item = m_treectrl->GetFirstChild(suite_item, cookie);
  while (true == test_item.IsOk()) {
    data = dynamic_cast<TreeTestData*>(m_treectrl->GetItemData(test_item));
    if (data->pTest == pTest) {
      break;
    }
    test_item = m_treectrl->GetNextChild(suite_item, cookie);
  }
  return test_item;

}

void TopFrame::DeactivateSuiteInfo(void)
{
  if (true == m_suite_info_active) {
    m_suite_info_box->SetForegroundColour("LIGHT GRAY");
    m_suite_info_box->Refresh();

    m_suite_name_label->SetForegroundColour("LIGHT GRAY");
    m_suite_name_label->Refresh();

    m_suite_name_text->SetForegroundColour("LIGHT GRAY");
    m_suite_name_text->Refresh();
    m_suite_name_text->SetValue(_("n/a"));

    m_suite_init_label->SetForegroundColour("LIGHT GRAY");
    m_suite_init_label->Refresh();

    m_suite_init_text->SetForegroundColour("LIGHT GRAY");
    m_suite_init_text->Refresh();
    m_suite_init_text->SetLabel(_("n/a"));

    m_suite_clean_label->SetForegroundColour("LIGHT GRAY");
    m_suite_clean_label->Refresh();

    m_suite_clean_text->SetForegroundColour("LIGHT GRAY");
    m_suite_clean_text->Refresh();
    m_suite_clean_text->SetLabel(_("n/a"));

    m_suite_active_box->SetForegroundColour("LIGHT GRAY");
    m_suite_active_box->Refresh();
    m_suite_active_box->Set3StateValue(wxCHK_UNDETERMINED);

    m_suite_info_active = false;
  }
}

void TopFrame::ActivateSuiteInfo(void)
{
  if (false == m_suite_info_active) {
    m_suite_info_box->SetForegroundColour("BLACK");
    m_suite_info_box->Refresh();

    m_suite_name_label->SetForegroundColour("BLACK");
    m_suite_name_label->Refresh();

    m_suite_name_text->SetForegroundColour("BLACKY");
    m_suite_name_text->Refresh();

    m_suite_init_label->SetForegroundColour("BLACK");
    m_suite_init_label->Refresh();

    m_suite_init_text->SetForegroundColour("BLACK");
    m_suite_init_text->Refresh();

    m_suite_clean_label->SetForegroundColour("BLACK");
    m_suite_clean_label->Refresh();

    m_suite_clean_text->SetForegroundColour("BLACK");
    m_suite_clean_text->Refresh();

    m_suite_active_box->SetForegroundColour("BLACK");
    m_suite_active_box->Refresh();

    m_suite_info_active = true;
  }
}

void TopFrame::DeactivateTestInfo(void)
{
  if (true == m_test_info_active) {
    m_test_info_box->SetForegroundColour("LIGHT GRAY");
    m_test_info_box->Refresh();

    m_test_name_label->SetForegroundColour("LIGHT GRAY");
    m_test_name_label->Refresh();

    m_test_name_text->SetForegroundColour("LIGHT GRAY");
    m_test_name_text->Refresh();
    m_test_name_text->SetLabel(_("n/a"));

    m_test_active_box->SetForegroundColour("LIGHT GRAY");
    m_test_active_box->Refresh();

    m_test_info_active = false;
  }
}

void TopFrame::ActivateTestInfo(void)
{
  if (false == m_test_info_active) {
    m_test_info_box->SetForegroundColour("BLACK");
    m_test_info_box->Refresh();

    m_test_name_label->SetForegroundColour("BLACK");
    m_test_name_label->Refresh();

    m_test_name_text->SetForegroundColour("BLACK");
    m_test_name_text->Refresh();

    m_test_active_box->SetForegroundColour("BLACK");
    m_test_active_box->Refresh();
    m_test_active_box->Set3StateValue(wxCHK_UNDETERMINED);

    m_test_info_active = true;
  }
}

void TopFrame::DeactivateRunResultInfo(void)
{
  if (true == m_result_info_active) {
    m_result_box->SetForegroundColour("LIGHT GRAY");
    m_result_box->Refresh();

    m_result_ntests_label->SetForegroundColour("LIGHT GRAY");
    m_result_ntests_label->Refresh();

    m_result_ntests_text->SetForegroundColour("LIGHT GRAY");
    m_result_ntests_text->Refresh();
    m_result_ntests_text->SetValue(_("n/a"));

    m_result_nasserts_label->SetForegroundColour("LIGHT GRAY");
    m_result_nasserts_label->Refresh();

    m_result_nasserts_text->SetForegroundColour("LIGHT GRAY");
    m_result_nasserts_text->Refresh();
    m_result_nasserts_text->SetValue(_("n/a"));

    m_result_nfailures_label->SetForegroundColour("LIGHT GRAY");
    m_result_nfailures_label->Refresh();

    m_result_nfailures_text->SetForegroundColour("LIGHT GRAY");
    m_result_nfailures_text->Refresh();
    m_result_nfailures_text->SetValue(_("n/a"));

    m_result_info_active = false;
  }

  if (true == m_failure_info_active) {
    m_failure_listing_box->SetForegroundColour("LIGHT GRAY");
    m_failure_listing_box->Refresh();

    m_failure_info_active = false;
  }
}

void TopFrame::ActivateRunResultInfo(void)
{
  if (false == m_result_info_active) {
    m_result_box->SetForegroundColour("BLACK");
    m_result_box->Refresh();

    m_result_ntests_label->SetForegroundColour("BLACK");
    m_result_ntests_label->Refresh();

    m_result_ntests_text->SetForegroundColour("BLACK");
    m_result_ntests_text->Refresh();

    m_result_nasserts_label->SetForegroundColour("BLACK");
    m_result_nasserts_label->Refresh();

    m_result_nasserts_text->SetForegroundColour("BLACK");
    m_result_nasserts_text->Refresh();

    m_result_nfailures_label->SetForegroundColour("BLACK");
    m_result_nfailures_label->Refresh();

    m_result_nfailures_text->SetForegroundColour("BLACK");
    m_result_nfailures_text->Refresh();

    m_result_info_active = true;
  }
  
  if (false == m_failure_info_active) {
    m_failure_listing_box->SetForegroundColour("BLACK");
    m_failure_listing_box->Refresh();

    m_failure_info_active = true;
  }
}

void TopFrame::WriteFailureRecords(wxTextCtrl *textctrl, 
                                   unsigned int nFailures, 
                                   CU_pFailureRecord pFailure,
                                   bool reset)
{
  static long failure_number = 0;
  
  assert(NULL != textctrl);
  
  if (true == reset) {
    textctrl->Clear();
    failure_number = 0;
  }
  
  for (unsigned int i=0 ; i<nFailures ; ++i) {
    if (NULL != pFailure) {
      *textctrl << ++failure_number << ".  "
                << ((NULL != pFailure->strCondition) ? pFailure->strCondition : _("<condition unspecified>"))
                << "\n" << _("Suite: ") 
                << (((NULL != pFailure->pSuite) &&
                     (NULL != pFailure->pSuite->pName)) ? pFailure->pSuite->pName : "")
                << "\n" << _("Test:  ")
                << (((NULL != pFailure->pTest) &&
                     (NULL != pFailure->pTest->pName)) ? pFailure->pTest->pName : "")
                << "\n" << _("Source: ") 
                << ((NULL != pFailure->strFunction) ? pFailure->strFunction : "")
                << ":" << ((NULL != pFailure->strFileName) ? pFailure->strFileName : "")
                << ":" << (long)(pFailure->uiLineNumber) << "\n\n";
      pFailure = pFailure->pNext;
    }
  }
  textctrl->ShowPosition(0);
}

TopFrame::TopFrame(const wxString& title, const wxPoint& pos, const wxSize& size)
: wxFrame((wxFrame *)NULL, -1, title, pos, size, wxDEFAULT_FRAME_STYLE), 
  m_option_colorize_tree_items(true),
  m_option_fail_on_inactive(true),
  m_test_run_complete(false),
  m_suite_info_active(true),    // set true so will be properly deactivated during function
  m_test_info_active(true),
  m_result_info_active(true),
  m_failure_info_active(true)
{
  wxBusyCursor wait;
  
  // INITIALIZE THE MENU SYSTEM
  // File menu
  wxMenu *menuFile = new wxMenu;

  menuFile->Append(menu_FileExit, _("E&xit"), _("Exit the program"));

  // View menu
  wxMenu *menuView = new wxMenu;

  menuView->Append(menu_ViewExpandAll, _("Ex&pand all\tCtrl+P"), _("Expand all suite nodes"));
  menuView->Append(menu_ViewCollapseAll, _("C&ollapse all\tCtrl+O"), _("Collapse all suite nodes"));
  menuView->AppendSeparator();
  menuView->AppendCheckItem(menu_ViewColorizeTreeItems, 
                            _("Use &color"), 
                            _("Change background colors to indicate run results."));
  menuView->Check(menu_ViewColorizeTreeItems, true);
  
  // Option menu
  wxMenu *menuOption = new wxMenu;

  menuOption->AppendCheckItem(menu_OptionFailOnInactive, 
                              _("Fail on &Inactive"), 
                              _("Generate failure events when an inactive suite or test is run."));

  menuOption->Check(menu_OptionFailOnInactive, true);
  menuOption->AppendSeparator();

  wxMenu *menuOptionActivate = new wxMenu;

  menuOptionActivate->Append(menu_OptionActivateSelected, _("&Item\tCtrl+A"), _("Activate the selected item"));
  menuOptionActivate->Append(menu_OptionActivateAllSuites, _("&Suites"), _("Activate all suites"));
  menuOptionActivate->Append(menu_OptionActivateAll, _("&All\tCtrl+Shift+A"), _("Activate all suites and tests"));

  wxMenu *menuOptionDeactivate = new wxMenu;

  menuOptionDeactivate->Append(menu_OptionDeactivateSelected, _("&Item\tCtrl+D"), _("Deactivate the selected item"));
  menuOptionDeactivate->Append(menu_OptionDeactivateAllSuites, _("&Suites"), _("Deactivate all suites"));
  menuOptionDeactivate->Append(menu_OptionDeactivateAll, _("&All\tCtrl+Shift+D"), _("Deactivate all suites and tests"));

  menuOption->Append(menu_OptionActivateSelected, _("&Activate"), menuOptionActivate);
  menuOption->Append(menu_OptionDeactivateSelected, _("&Deactivate"), menuOptionDeactivate);

  // Run menu
  wxMenu *menuRun = new wxMenu;

  menuRun->Append(menu_RunRunAll, _("&Run all\tCtrl+r"), _("Run all registered suites"));

  // Help menu
  wxMenu *menuHelp = new wxMenu;

  menuHelp->AppendSeparator();
  menuHelp->Append(menu_HelpAbout, _("&About..."), _("About CUnit"));

  // Apply menu bar to frame
  wxMenuBar *menuBar = new wxMenuBar;
  menuBar->Append(menuFile, _("&File"));
  menuBar->Append(menuView, _("&View"));
  menuBar->Append(menuOption, _("&Option"));
  menuBar->Append(menuRun, _("&Run"));
  menuBar->Append(menuHelp, _("&Help"));

  SetMenuBar( menuBar );

  // CREATE && INITIALIZE THE STATUS BAR
  CreateStatusBar();
  SetStatusText(_("Welcome to CUnit!"));

  // CREATE & INITIALIZE IMAGE LIST FOR LISTING ICONS
  wxImageList *images = new wxImageList(16, 16, true);

  // should correspond to TreeCtrlIcon_xxx enum
  images->Add(wxBitmap(wxBitmap(wxIcon(icon_suite_active_xpm)).ConvertToImage().Rescale(16, 16)));
  images->Add(wxBitmap(wxBitmap(wxIcon(icon_suite_active_open_xpm)).ConvertToImage().Rescale(16, 16)));
  images->Add(wxBitmap(wxBitmap(wxIcon(icon_suite_inactive_xpm)).ConvertToImage().Rescale(16, 16)));
  images->Add(wxBitmap(wxBitmap(wxIcon(icon_suite_inactive_open_xpm)).ConvertToImage().Rescale(16, 16)));
  images->Add(wxBitmap(wxBitmap(wxIcon(icon_test_active_xpm)).ConvertToImage().Rescale(16, 16)));
  images->Add(wxBitmap(wxBitmap(wxIcon(icon_test_inactive_xpm)).ConvertToImage().Rescale(16, 16)));
  images->Add(wxBitmap(wxBitmap(wxIcon(icon_test_active_suite_inactive_xpm)).ConvertToImage().Rescale(16, 16)));
  images->Add(wxBitmap(wxBitmap(wxIcon(icon_test_inactive_suite_inactive_xpm)).ConvertToImage().Rescale(16, 16)));
   
  // CREATE CONTROLS   
  wxSizer *topsizer = new wxBoxSizer(wxHORIZONTAL);

  // create tree for suite & test listing and add to sizer
  m_treectrl = new wxTreeCtrl(this, 
                              tree_Listing, 
                              wxDefaultPosition, 
                              wxDefaultSize,
                              wxTR_DEFAULT_STYLE | wxTR_HIDE_ROOT );
  m_treectrl->AssignImageList(images);
  topsizer->Add(m_treectrl, 2, wxEXPAND | wxALL, 10);

  // create a sizer to manage page 1 of the notebook
  wxBoxSizer *page1sizer = new wxBoxSizer(wxVERTICAL);

  wxNotebook *notebook = new wxNotebook(this, -1);
  wxPanel *panelinfo = new wxPanel(notebook, -1);

  this->SetBackgroundColour(notebook->GetBackgroundColour());

  // create controls for suite information
  m_suite_info_box = new wxStaticBox(panelinfo, -1, _("Suite Information"));
  wxSizer *suiteinfosizer = new wxStaticBoxSizer(m_suite_info_box, wxVERTICAL);

  wxSizer *bsizerH = new wxBoxSizer(wxHORIZONTAL);
  m_suite_name_label = new wxStaticText(panelinfo, -1, _("Name:"), 
                                        wxDefaultPosition, 
                                        wxSize(75,-1), 
                                        wxALIGN_RIGHT);
  bsizerH->Add(m_suite_name_label, 0, wxEXPAND | wxALL, 2);
  m_suite_name_text = new wxTextCtrl(panelinfo, -1, _("n/a"),
                                     wxDefaultPosition,
                                     wxDefaultSize,
                                     wxNO_BORDER | wxTE_READONLY);
  m_suite_name_text->SetBackgroundColour(panelinfo->GetBackgroundColour());
  bsizerH->Add(m_suite_name_text, 1, wxEXPAND | wxALL, 2);
  suiteinfosizer->Add(bsizerH, 0, wxEXPAND | wxALL, 1);
  
  bsizerH = new wxBoxSizer(wxHORIZONTAL);
  m_suite_init_label = new wxStaticText(panelinfo, -1, _("Init Func?"), 
                                        wxDefaultPosition, 
                                        wxSize(75,-1), 
                                        wxALIGN_RIGHT);
  bsizerH->Add(m_suite_init_label, 0, wxEXPAND | wxALL, 2);
  m_suite_init_text = new wxStaticText(panelinfo, -1, _("n/a"),
                                       wxDefaultPosition, 
                                       wxDefaultSize, 
                                       wxST_NO_AUTORESIZE);
  bsizerH->Add(m_suite_init_text, 1, wxEXPAND | wxALL, 2);
  suiteinfosizer->Add(bsizerH, 0, wxEXPAND | wxALL, 1);
               
  bsizerH = new wxBoxSizer(wxHORIZONTAL);
  m_suite_clean_label = new wxStaticText(panelinfo, -1, _("Cleanup Func?"), 
                                         wxDefaultPosition, 
                                         wxSize(75,-1), 
                                         wxALIGN_RIGHT);
  bsizerH->Add(m_suite_clean_label, 0, wxEXPAND | wxALL, 2);
  m_suite_clean_text = new wxStaticText(panelinfo, -1, _("n/a"),
                                        wxDefaultPosition, 
                                        wxDefaultSize, 
                                        wxST_NO_AUTORESIZE);
  bsizerH->Add(m_suite_clean_text, 1, wxEXPAND | wxALL, 2);
  suiteinfosizer->Add(bsizerH, 0, wxEXPAND | wxALL, 1);

  bsizerH = new wxBoxSizer(wxHORIZONTAL);
  bsizerH->Add(25, 5, 0, wxGROW | wxALL, 2); // spacer
  m_suite_active_box = new wxCheckBox(panelinfo, 
                                      checkbox_SuiteActive,  
                                      _("Active?"),
                                      wxDefaultPosition, 
                                      wxDefaultSize, 
                                      wxCHK_3STATE | wxALIGN_RIGHT);
  bsizerH->Add(m_suite_active_box, 0, wxALIGN_LEFT | wxEXPAND | wxALL, 1);
  suiteinfosizer->Add(bsizerH, 0, wxEXPAND | wxALL, 1);

  page1sizer->Add(suiteinfosizer, 0, wxEXPAND | wxALL, 1);

  // create controls for test information
  m_test_info_box = new wxStaticBox(panelinfo, -1, _("Test Information"));
  wxSizer *testinfosizer = new wxStaticBoxSizer(m_test_info_box, wxVERTICAL);

  bsizerH = new wxBoxSizer(wxHORIZONTAL);
  m_test_name_label = new wxStaticText(panelinfo, -1, _("Name:"), 
                                       wxDefaultPosition, 
                                       wxSize(75,-1), 
                                       wxALIGN_RIGHT);
  bsizerH->Add(m_test_name_label, 0, wxEXPAND | wxALL, 2);
  m_test_name_text = new wxTextCtrl(panelinfo, -1, _("n/a"),
                                    wxDefaultPosition,
                                    wxDefaultSize,
                                    wxNO_BORDER | wxTE_READONLY);
  m_test_name_text->SetBackgroundColour(panelinfo->GetBackgroundColour());
  bsizerH->Add(m_test_name_text, 1, wxEXPAND | wxALL, 2);
  testinfosizer->Add(bsizerH, 0, wxEXPAND | wxALL, 1);
  
  bsizerH = new wxBoxSizer(wxHORIZONTAL);
  bsizerH->Add(25, 5, 0, wxGROW | wxALL, 1); // spacer
  m_test_active_box = new wxCheckBox(panelinfo, 
                                     checkbox_TestActive,  
                                     _("Active?"),
                                     wxDefaultPosition, 
                                     wxDefaultSize, 
                                     wxCHK_3STATE | wxALIGN_RIGHT);
  bsizerH->Add(m_test_active_box, 0, wxALIGN_LEFT | wxEXPAND | wxALL, 1);
  testinfosizer->Add(bsizerH, 0, wxEXPAND | wxALL, 1);

  page1sizer->Add(testinfosizer, 0, wxEXPAND | wxALL, 1);

  // create controls for run result information
  m_result_box = new wxStaticBox(panelinfo, -1, _("Run Result Information"));
  wxSizer *resultinfosizer = new wxStaticBoxSizer(m_result_box, wxVERTICAL);
  
  bsizerH = new wxBoxSizer(wxHORIZONTAL);
  m_result_ntests_label = new wxStaticText(panelinfo, -1, _("# Tests:"), 
                                          wxDefaultPosition, 
                                          wxSize(75,-1), 
                                          wxALIGN_RIGHT);
  bsizerH->Add(m_result_ntests_label, 0, wxEXPAND | wxALL, 2);
  m_result_ntests_text = new wxTextCtrl(panelinfo, -1, _("n/a"),
                                       wxDefaultPosition,
                                       wxDefaultSize,
                                       wxNO_BORDER | wxTE_READONLY);
  m_result_ntests_text->SetBackgroundColour(panelinfo->GetBackgroundColour());
  bsizerH->Add(m_result_ntests_text, 1, wxEXPAND | wxALL, 2);
  resultinfosizer->Add(bsizerH, 0, wxEXPAND | wxALL, 1);
  
  bsizerH = new wxBoxSizer(wxHORIZONTAL);
  m_result_nasserts_label = new wxStaticText(panelinfo, -1, _("# Asserts:"), 
                                            wxDefaultPosition, 
                                            wxSize(75,-1), 
                                            wxALIGN_RIGHT);
  bsizerH->Add(m_result_nasserts_label, 0, wxEXPAND | wxALL, 2);
  m_result_nasserts_text = new wxTextCtrl(panelinfo, -1, _("n/a"),
                                         wxDefaultPosition,
                                         wxDefaultSize,
                                         wxNO_BORDER | wxTE_READONLY);
  m_result_nasserts_text->SetBackgroundColour(panelinfo->GetBackgroundColour());
  bsizerH->Add(m_result_nasserts_text, 1, wxEXPAND | wxALL, 2);
  resultinfosizer->Add(bsizerH, 0, wxEXPAND | wxALL, 1);

  
  bsizerH = new wxBoxSizer(wxHORIZONTAL);
  m_result_nfailures_label = new wxStaticText(panelinfo, -1, _("# Failures:"), 
                                             wxDefaultPosition, 
                                             wxSize(75,-1), 
                                             wxALIGN_RIGHT);
  bsizerH->Add(m_result_nfailures_label, 0, wxEXPAND | wxALL, 2);
  m_result_nfailures_text = new wxTextCtrl(panelinfo, -1, _("n/a"),
                                           wxDefaultPosition,
                                           wxDefaultSize,
                                           wxNO_BORDER | wxTE_READONLY);
  m_result_nfailures_text->SetBackgroundColour(panelinfo->GetBackgroundColour());
  bsizerH->Add(m_result_nfailures_text, 1, wxEXPAND | wxALL, 2);
  resultinfosizer->Add(bsizerH, 0, wxEXPAND | wxALL, 1);

  page1sizer->Add(resultinfosizer, 0, wxEXPAND | wxALL, 1);

  // create controls for failure listing
  m_failure_listing_box = new wxStaticBox(panelinfo, -1, _("Failure Listing"));
  wxSizer *failurelistingsizer = new wxStaticBoxSizer(m_failure_listing_box, wxVERTICAL);

  m_failure_listing_text = new wxTextCtrl(panelinfo, -1, "",
                                          wxDefaultPosition,
                                          wxDefaultSize,
                                          wxTE_MULTILINE | wxTE_READONLY);
  failurelistingsizer->Add(m_failure_listing_text, 1, wxEXPAND | wxALL, 1);

  page1sizer->Add(failurelistingsizer, 1, wxEXPAND | wxALL, 1);

  // add panelinfo to notebook
  panelinfo->SetSizer(page1sizer);
  notebook->AddPage(panelinfo, _("Info"), true);

  // create a sizer to manage page 2 of the notebook
  m_page2sizer = new wxBoxSizer(wxVERTICAL);
  wxPanel *panelrunsummary = new wxPanel(notebook, -1);

  // create controls to display run summary & failure listing
  m_run_summary_text = new wxStaticText(panelrunsummary, -1, "", 
                                        wxDefaultPosition, 
                                        wxSize(75,-1), 
                                        wxALIGN_LEFT);
  m_run_summary_text->SetFont(wxFont(notebook->GetFont().GetPointSize(), 
                              wxFONTFAMILY_MODERN, 
                              wxFONTSTYLE_NORMAL, 
                              wxFONTWEIGHT_NORMAL));
  m_page2sizer->Add(m_run_summary_text, 0, wxEXPAND | wxALL, 20);
  
  wxSizer *allfailurelistingsizer = new wxStaticBoxSizer(wxVERTICAL, panelrunsummary, _("Failure Listing"));

  m_all_failure_listing_text = new wxTextCtrl(panelrunsummary, -1, "",
                                              wxDefaultPosition,
                                              wxDefaultSize,
                                              wxTE_MULTILINE | wxTE_READONLY);
  allfailurelistingsizer->Add(m_all_failure_listing_text, 1, wxEXPAND | wxALL, 1);

  m_page2sizer->Add(allfailurelistingsizer, 1, wxEXPAND | wxALL, 1);

  // add panelrunsummary to notebook
  panelrunsummary->SetSizer(m_page2sizer);
  notebook->AddPage(panelrunsummary, _("Run Summary"), false);


  // add notebook to topsizer
  topsizer->Add(notebook, 3, wxEXPAND | wxALL, 10);
  
  SetSizer(topsizer);      // use the sizer for layout

  topsizer->SetSizeHints(this);   // set size hints to honour minimum size

  DeactivateSuiteInfo();
  DeactivateTestInfo();
  DeactivateRunResultInfo();

  // CREATE TESTS AND FILL IN TREE LISTING
  if (CU_TRUE == create_tests()) {

    CU_pTestRegistry pRegistry = CU_get_registry();
    assert(NULL != pRegistry);
    
    wxTreeItemId root_item = m_treectrl->AddRoot(_("Test Registry"));
    CU_pSuite pSuite = pRegistry->pSuite;
    while (NULL != pSuite) {
      wxTreeItemId suite_item = m_treectrl->AppendItem(root_item, 
                                                       pSuite->pName,
                                                       -1,
                                                       -1,
                                                       new TreeSuiteData(pSuite));

      FormatTreeItem_Suite(m_treectrl, suite_item, (CU_TRUE == pSuite->fActive));

      CU_pTest pTest = pSuite->pTest;
      while (NULL != pTest) {
        wxTreeItemId test_item = m_treectrl->AppendItem(suite_item, 
                                                        pTest->pName,
                                                        -1,
                                                        -1,
                                                        new TreeTestData(pTest));
        FormatTreeItem_Test(m_treectrl, 
                            test_item, 
                            (CU_TRUE == pTest->fActive),
                            (CU_TRUE == pSuite->fActive));

        pTest = pTest->pNext;
      }
      pSuite = pSuite->pNext;
    }
  }
  else {
    wxMessageBox(_("Tests creation failed!"), 
                 _("Test Creation Report"), 
                 wxOK | wxICON_EXCLAMATION);
  } 
  
  // MAKE SURE CUNIT OPTIONS COORDINATED WITH INTERFACE
  CU_set_fail_on_inactive((true == m_option_fail_on_inactive) ? CU_TRUE : CU_FALSE);
}

void TopFrame::OnQuit(wxCommandEvent& WXUNUSED(event))
{
  Close(true);
}

void TopFrame::OnCloseWindow(wxCloseEvent& WXUNUSED(event))
{
  if (CU_FALSE == destroy_tests()) {
    wxMessageBox(_("Tests destruction failed!"), 
                 _("Test Destruction Report"), 
                 wxOK | wxICON_EXCLAMATION);
  }

  this->Destroy();
}

void TopFrame::OnAbout(wxCommandEvent& WXUNUSED(event))
{
  static wxString msg;
  
  if (msg.Len() == 0) {
    msg << _("CUnit - A Unit Testing Framework for C") << "\n"
        << _("http://cunit.sourceforge.net/") << "\n\n"
        << _("Copyright (c) 2001-2006 Anil Kumar, Jerry St.Clair") << "\n\n"
        << _("Made with wxWidgets:") << "\n"
        << _("http://www.wxwidgets.org");
  }
                      
  wxMessageBox(msg, _("About CUnit"), wxOK | wxICON_INFORMATION);
}

void TopFrame::OnTreeExpandAll(wxCommandEvent& WXUNUSED(event))
{
  ForEachSuite(m_treectrl, f_expand_item_functor);
}

void TopFrame::OnTreeCollapseAll(wxCommandEvent& WXUNUSED(event))
{
  ForEachSuite(m_treectrl, f_collapse_item_functor);
}

void TopFrame::OnTreeSelectionChanged(wxTreeEvent& event)
{
  wxTreeItemId suite_item = event.GetItem();
  wxTreeItemId test_item;

  if (false == suite_item.IsOk()) {
    DeactivateSuiteInfo();
    DeactivateTestInfo();
    DeactivateRunResultInfo();
    return;
  }
    
  // Action depends on whether the item selected is a suite or test
  if (isTest(m_treectrl, suite_item)) {
    test_item = suite_item;
    suite_item = m_treectrl->GetItemParent(test_item);
  }
  
  // fill in suite info
  TreeSuiteData *data = dynamic_cast<TreeSuiteData*>(m_treectrl->GetItemData(suite_item));
  CU_pSuite pSuite = data->pSuite;

  ActivateSuiteInfo();
  m_suite_name_text->SetValue(pSuite->pName);
  m_suite_init_text->SetLabel(wxString(((NULL != pSuite->pInitializeFunc) ? _("Yes") : _("No"))));
  m_suite_clean_text->SetLabel(wxString(((NULL != pSuite->pCleanupFunc) ? _("Yes") : _("No"))));
  m_suite_active_box->SetValue((CU_FALSE == pSuite->fActive) ? false : true);

  if (false == m_test_run_complete) {
    DeactivateRunResultInfo();
  }
  else {
    ActivateRunResultInfo();
  }
  
  if (true == test_item.IsOk()) {
    // selected item is a test - fill in test info
    TreeTestData *data = dynamic_cast<TreeTestData*>(m_treectrl->GetItemData(test_item));
    CU_pTest pTest = data->pTest;
    ActivateTestInfo();
        
    // update test information
    m_test_name_text->SetValue(pTest->pName);
    m_test_active_box->SetValue((CU_FALSE == pTest->fActive) ? false : true);
    // update run result information
    if (true == m_test_run_complete) {
      m_result_ntests_label->SetForegroundColour("LIGHT GRAY");
      m_result_ntests_label->Refresh();
      m_result_ntests_text->SetForegroundColour("LIGHT GRAY");
      m_result_ntests_text->Refresh();
      m_result_ntests_text->SetValue(_("n/a"));

      wxString text;
      text << data->nAsserts;
      m_result_nasserts_text->SetValue(text);
      text.Clear();      
      text << data->nFailures;
      m_result_nfailures_text->SetValue(text);
      
      if (0 == data->nFailures) {
        m_failure_listing_text->SetValue("No failures.");
      }
      else {
        WriteFailureRecords(m_failure_listing_text, data->nFailures, data->pFirstFailure, true);
      }
    }
  }
  else {
    // a suite is selected - inactivate test information
    DeactivateTestInfo();

    // update result information for the suite
    if (true == m_test_run_complete) {
      m_result_ntests_label->SetForegroundColour("BLACK");
      m_result_ntests_label->Refresh();
      m_result_ntests_text->SetForegroundColour("BLACK");
      m_result_ntests_text->Refresh();

      wxString text;
      text << data->nTestsRun;
      m_result_ntests_text->SetValue(text);
      text.Clear();      
      text << data->nAsserts;
      m_result_nasserts_text->SetValue(text);
      text.Clear();      
      text << data->nFailures;
      m_result_nfailures_text->SetValue(text);

      if (0 == data->nFailures) {
        m_failure_listing_text->SetValue("No failures.");
      }
      else {
        WriteFailureRecords(m_failure_listing_text, data->nFailures, data->pFirstFailure, true);
      }
    }
  }
}

void TopFrame::OnActivateSelected(wxCommandEvent& WXUNUSED(event))
{
  wxTreeItemId item = m_treectrl->GetSelection();
  if (item.IsOk()) {
    if (isSuite(m_treectrl, item)) {
      f_activate_suite_item_functor.func(m_treectrl, item);
    }
    else {
      f_activate_test_item_functor.func(m_treectrl, item);
    }
    m_treectrl->SelectItem(item);   // re-select item to make sure display is updated
  }
}

void TopFrame::OnDeactivateSelected(wxCommandEvent& WXUNUSED(event))
{
  wxTreeItemId item = m_treectrl->GetSelection();
  if (item.IsOk()) {
    if (isSuite(m_treectrl, item)) {
      f_deactivate_suite_item_functor.func(m_treectrl, item);
    }
    else {
      f_deactivate_test_item_functor.func(m_treectrl, item);
    }
    m_treectrl->SelectItem(item);   // re-select item to make sure display is updated
  }
}

void TopFrame::OnActivateAllSuites(wxCommandEvent& WXUNUSED(event))
{
  ForEachSuite(m_treectrl, f_activate_suite_item_functor);
  m_treectrl->SelectItem(m_treectrl->GetSelection());   // make sure display is updated
}

void TopFrame::OnDeactivateAllSuites(wxCommandEvent& WXUNUSED(event))
{
  ForEachSuite(m_treectrl, f_deactivate_suite_item_functor);
  m_treectrl->SelectItem(m_treectrl->GetSelection());   // make sure display is updated
}

void TopFrame::OnActivateAll(wxCommandEvent& WXUNUSED(event))
{
  ForEachSuiteAndTest(m_treectrl, 
                      f_activate_suite_item_functor, 
                      f_activate_test_item_functor);
  m_treectrl->SelectItem(m_treectrl->GetSelection());   // make sure display is updated
}

void TopFrame::OnDeactivateAll(wxCommandEvent& WXUNUSED(event))
{
  ForEachSuiteAndTest(m_treectrl, 
                      f_deactivate_suite_item_functor, 
                      f_deactivate_test_item_functor);
  m_treectrl->SelectItem(m_treectrl->GetSelection());   // make sure display is updated
}

void TopFrame::OnRunAll(wxCommandEvent& WXUNUSED(event))
{
  // set up CUnit test run message handlers
  CU_set_suite_start_handler(&TopFrame::SuiteStartMessageHandler);
  CU_set_test_start_handler(&TopFrame::TestStartMessageHandler);
  CU_set_test_complete_handler(&TopFrame::TestCompleteMessageHandler);
  CU_set_suite_complete_handler(&TopFrame::SuiteCompleteMessageHandler);
  // this is the active TopFrame to be updated with run results
  m_ActiveTopFrame = this;
  
  // clear previous stats and run tests
  ClearPreviousStats();
  CU_run_all_tests();
  m_test_run_complete = true;
  m_treectrl->SelectItem(m_treectrl->GetSelection());  // trigger updating the results info
  
  // fill in run summary results
  char *summary_cstring = CU_get_run_results_string();
  wxString summary_string(summary_cstring);
  summary_string << "\n" << _("Failure count = ") << CU_get_number_of_failure_records();
  CU_FREE(summary_cstring);
  
  m_run_summary_text->SetLabel(summary_string);
  if (0 == CU_get_number_of_failure_records()) {
    m_all_failure_listing_text->SetValue("No failures.");
  }
  else {
    WriteFailureRecords(m_all_failure_listing_text, 
                        CU_get_number_of_failure_records(), 
                        CU_get_failure_list(), 
                        true);
  }
  m_page2sizer->Layout();
}

void TopFrame::OnOptionColorizeTreeItems(wxCommandEvent& event)
{
  if (event.IsChecked() && (false == m_option_colorize_tree_items)) {
    m_option_colorize_tree_items = true;
    if (m_test_run_complete) {
      ForEachSuiteAndTest(m_treectrl, f_colorize_item_background, f_colorize_item_background);
    }
  }
  else if (!event.IsChecked() && (true == m_option_colorize_tree_items)) {
    m_option_colorize_tree_items = false;
    if (m_test_run_complete) {
      ForEachSuiteAndTest(m_treectrl, f_reset_item_background_color, f_reset_item_background_color);
    }
  }
}

void TopFrame::OnOptionFailOnInactive(wxCommandEvent& event)
{
  if (event.IsChecked() && (false == m_option_fail_on_inactive)) {
    m_option_fail_on_inactive = true;
    CU_set_fail_on_inactive(CU_TRUE);
  }
  else if (!event.IsChecked() && (true == m_option_colorize_tree_items)) {
    m_option_fail_on_inactive = false;
    CU_set_fail_on_inactive(CU_FALSE);
  }
}

void TopFrame::OnSuiteActiveCheckbox(wxCommandEvent& event)
{
  wxTreeItemId suite_item = m_treectrl->GetSelection();

  if ((true == m_suite_info_active) && (true == suite_item.IsOk())) {
      
    // If a test is selected, act on it's suite
    if (isTest(m_treectrl, suite_item)) {
      suite_item = m_treectrl->GetItemParent(suite_item);
    }
    
    CU_pSuite pSuite = dynamic_cast<TreeSuiteData*>(m_treectrl->GetItemData(suite_item))->pSuite;
    pSuite->fActive = (true == event.IsChecked()) ? CU_TRUE : CU_FALSE;
  }
  else {
    m_suite_active_box->Set3StateValue(wxCHK_UNDETERMINED);
  }
}

void TopFrame::OnTestActiveCheckbox(wxCommandEvent& event)
{
  wxTreeItemId test_item = m_treectrl->GetSelection();

  if ((true == m_test_info_active) && (true == test_item.IsOk())) {
    CU_pTest pTest = dynamic_cast<TreeTestData*>(m_treectrl->GetItemData(test_item))->pTest;
    pTest->fActive = (true == event.IsChecked()) ? CU_TRUE : CU_FALSE;
  }
  else {
    m_test_active_box->Set3StateValue(wxCHK_UNDETERMINED);
  }
}

void TopFrame::SuiteStartMessageHandler(const CU_pSuite pSuite)
{
  CU_UNREFERENCED_PARAMETER(pSuite);

  assert(NULL != m_ActiveTopFrame);
  m_ActiveTopFrame->m_RunSummary_suite = *(CU_get_run_summary());
}

void TopFrame::TestStartMessageHandler(const CU_pTest pTest, const CU_pSuite pSuite)
{
  CU_UNREFERENCED_PARAMETER(pTest);
  CU_UNREFERENCED_PARAMETER(pSuite);

  assert(NULL != m_ActiveTopFrame);
  m_ActiveTopFrame->m_RunSummary_test = *(CU_get_run_summary());
}

void TopFrame::TestCompleteMessageHandler(const CU_pTest pTest, 
                                           const CU_pSuite pSuite,
                                           const CU_pFailureRecord pFailure)
{
  assert(NULL != m_ActiveTopFrame);

  wxTreeItemId test_item = m_ActiveTopFrame->FindTest(pSuite, pTest);
  
  if (test_item.IsOk()) {
    TopFrame *frame = m_ActiveTopFrame;
    wxTreeCtrl *tree = frame->m_treectrl;
    TreeTestData *data = dynamic_cast<TreeTestData*>(tree->GetItemData(test_item));
    data->nAsserts = CU_get_number_of_asserts() - frame->m_RunSummary_test.nAsserts;
    data->nFailures = CU_get_number_of_failure_records() - frame->m_RunSummary_test.nFailureRecords;
    data->pFirstFailure = pFailure;
    
    if (true == frame->m_option_colorize_tree_items) {
      f_colorize_item_background.func(tree, test_item);
    }
  }
}

void TopFrame::SuiteCompleteMessageHandler(const CU_pSuite pSuite,
                                           const CU_pFailureRecord pFailure)
{
  assert(NULL != m_ActiveTopFrame);

  wxTreeItemId suite_item = m_ActiveTopFrame->FindSuite(pSuite);
  
  if (suite_item.IsOk()) {
    TopFrame *frame = m_ActiveTopFrame;
    wxTreeCtrl *tree = frame->m_treectrl;
    TreeSuiteData *data = dynamic_cast<TreeSuiteData*>(tree->GetItemData(suite_item));
    data->nTestsRun = CU_get_number_of_tests_run() - frame->m_RunSummary_suite.nTestsRun;
    data->nAsserts  = CU_get_number_of_asserts() - frame->m_RunSummary_suite.nAsserts;
    data->nFailures = CU_get_number_of_failure_records() - frame->m_RunSummary_suite.nFailureRecords;
    data->pFirstFailure = pFailure;

    if (true == frame->m_option_colorize_tree_items) {
      f_colorize_item_background.func(tree, suite_item);
    }
  }
}

/*
void TopFrame::OnButtonOk(wxCommandEvent& WXUNUSED(event))
{
  wxString report;
  wxTreeItemId item = m_treectrl->GetSelection();
  
  if (isSuite(m_treectrl, item)) {
    TreeSuiteData *data = dynamic_cast<TreeSuiteData*>(m_treectrl->GetItemData(item));
    report << "Suite run summany:\n\n"
           << "Suite name: " << data->pSuite->pName << "\n"
           << "# Tests run: " << data->nTestsRun << "\n"
           << "# Asserts: " << data->nAsserts << "\n"
           << "# Failures: " << data->nFailures;
  }
  else if (isTest(m_treectrl, item)) {
    TreeTestData *data = dynamic_cast<TreeTestData*>(m_treectrl->GetItemData(item));
    report << "Test run summany:\n\n"
           << "Test name: " << data->pTest->pName << "\n"
           << "# Asserts: " << data->nAsserts << "\n"
           << "# Failures: " << data->nFailures;
  }
  
  wxMessageBox(report, "Run summary for selected node", wxOK);  
}

void TopFrame::OnButtonCancel(wxCommandEvent& WXUNUSED(event))
{
}
*/

/** @} */
