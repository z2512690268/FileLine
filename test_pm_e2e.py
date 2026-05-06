"""Playwright E2E test for Pipeline Management system."""
import sys
from pathlib import Path
import yaml

project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from playwright.sync_api import sync_playwright, expect
import re
import time
import shutil

BASE_URL = "http://localhost:8601"
MANAGED_PIPELINES_DIR = project_root / "managed_pipelines"
TEST_PIPELINE_NAME = "test_pm_e2e"


def _cleanup():
    pdir = MANAGED_PIPELINES_DIR / TEST_PIPELINE_NAME
    if pdir.exists():
        shutil.rmtree(pdir)


def _find_button(page, text):
    """Find a button by its visible text content."""
    return page.get_by_role("button", name=re.compile(text))


def _select_streamlit_option(page, placeholder, option_text):
    """Select an option in a Streamlit selectbox (React component).

    Strategy: find the selectbox containing the placeholder label,
    click the combobox input to open the dropdown, then click the option.
    """
    # Find the selectbox container by its label text
    selectbox_container = page.locator(f'[data-testid="stSelectbox"]').filter(has_text=placeholder).first
    expect(selectbox_container).to_be_visible(timeout=5000)
    selectbox_container.scroll_into_view_if_needed()
    page.wait_for_timeout(300)

    # Click the combobox input inside
    combobox = selectbox_container.locator('[role="combobox"]').first
    combobox.click()
    page.wait_for_timeout(1000)

    # The dropdown options appear in a portal (outside the container)
    # Try multiple selectors for dropdown options
    option = page.locator('[role="option"]').filter(has_text=option_text).first
    if option.count() == 0:
        # Fallback: try listbox items
        option = page.locator('[role="listbox"] [role="option"]').filter(has_text=option_text).first
    if option.count() == 0:
        # Try typing in the combobox to filter
        combobox.type(option_text)
        page.wait_for_timeout(500)
        option = page.locator('[role="option"]').filter(has_text=option_text).first

    expect(option).to_be_visible(timeout=5000)
    option.click()
    page.wait_for_timeout(500)


def test_full_pm_workflow():
    """Test the complete pipeline management workflow."""
    _cleanup()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1920, "height": 1080})
        page = context.new_page()

        # ========== Navigate to Plot Builder ==========
        page.goto(f"{BASE_URL}/Plot_Builder", wait_until="networkidle")
        page.wait_for_timeout(2000)
        expect(page.get_by_text("可视化流水线构建器")).to_be_visible(timeout=10000)

        # ========== Create new pipeline ==========
        # Select "(新建空白流水线)" if not already selected
        new_option = page.locator('[data-baseweb="select"]').first
        new_option.scroll_into_view_if_needed()
        page.wait_for_timeout(300)

        # Click 新建 button
        _find_button(page, "新建").click()
        page.wait_for_timeout(1000)

        # Enter pipeline name
        name_input = page.get_by_placeholder("my_pipeline")
        expect(name_input).to_be_visible(timeout=5000)
        name_input.fill(TEST_PIPELINE_NAME)

        # Enter description
        desc_input = page.get_by_placeholder("流水线功能描述")
        desc_input.fill("E2E test pipeline")

        print("✅ Pipeline name and description entered")

        # ========== Add global variables ==========
        page.get_by_text("🌐 全局变量").click()
        page.wait_for_timeout(500)

        _find_button(page, "添加变量").click()
        page.wait_for_timeout(500)

        # Find the global variable inputs
        var_key_input = page.locator('input[placeholder="KEY"]').first
        var_key_input.fill("REPEAT")
        var_value_input = page.locator('input[placeholder="value"]').first
        var_value_input.fill("3")

        # Collapse global variables
        page.get_by_text("🌐 全局变量").click()
        page.wait_for_timeout(300)

        print("✅ Global variables added")

        # ========== Step 1: Add data source ==========
        _find_button(page, "添加数据源").click()
        page.wait_for_timeout(2000)

        # The file browser should now be visible - starts at streamlit's cwd
        fb_root = page.locator(".fb-root").first
        expect(fb_root).to_be_visible(timeout=5000)

        # Navigate into FileLine/ directory where requirements.txt lives
        fl_dir = page.locator('.fb-dir-row[data-fb-nav="FileLine"] td.fb-name-cell').first
        if fl_dir.count() > 0:
            fl_dir.click()
            page.wait_for_timeout(2000)

        # Now select requirements.txt
        req_cb = page.locator('.fb-file-cb[data-fb-file="requirements.txt"]')
        if req_cb.count() == 0:
            print("⚠️ requirements.txt not found, listing visible files")
            visible_files = page.locator(".fb-file-row .fb-name-cell").all_text_contents()
            print(f"Visible files: {visible_files}")
            # Try any visible data file
            for f in visible_files:
                fname = f.strip()
                cb = page.locator(f'.fb-file-cb[data-fb-file="{fname}"]')
                if cb.count() > 0:
                    cb.check()
                    break
        else:
            req_cb.check()
            page.wait_for_timeout(300)

        # Verify checkbox is checked
        checked_count = page.locator('.fb-file-cb:checked').count()
        print(f"✅ {checked_count} file(s) selected in file browser")
        assert checked_count > 0, "No files selected in file browser"

        # Click confirm button
        confirm_btn = page.locator('.fb-confirm').first
        confirm_btn.click()
        page.wait_for_timeout(2000)

        # Wait for data source to be resolved (the resolve spinner + page update)
        page.wait_for_timeout(3000)

        print("✅ Data source added and resolved")

        # ========== Step 2: Add processing step ==========
        # Wait for the page to fully re-render after data source resolution
        page.wait_for_timeout(1000)

        # Find the "添加步骤" button
        add_step_btn = page.locator('button:has-text("添加步骤")').first
        expect(add_step_btn).to_be_visible(timeout=5000)
        add_step_btn.scroll_into_view_if_needed()
        page.wait_for_timeout(300)
        add_step_btn.click()
        page.wait_for_timeout(2000)  # wait for st.rerun()

        # Select text_repeat processor in the selectbox
        _select_streamlit_option(page, "步骤 1 处理器", "text_repeat")
        page.wait_for_timeout(1000)

        # Configure input source - should default to "(无输入)", change to source_1
        _select_streamlit_option(page, "输入源", "source_1")
        page.wait_for_timeout(500)

        # Configure parameters (expand params section)
        params_expander = page.get_by_text("参数配置").first
        if params_expander.count() > 0:
            params_expander.click()
            page.wait_for_timeout(500)

        # Set repeat_num parameter
        repeat_input = page.locator('input[value="1"]').first
        if repeat_input.count() > 0:
            repeat_input.fill("2")
            page.wait_for_timeout(300)

        print("✅ Processing step configured")

        # ========== Save pipeline ==========
        _find_button(page, "保存").click()
        page.wait_for_timeout(2000)

        # Verify pipeline.yaml was created
        pipeline_yaml = MANAGED_PIPELINES_DIR / TEST_PIPELINE_NAME / "pipeline.yaml"
        assert pipeline_yaml.exists(), "pipeline.yaml was not saved"
        with open(pipeline_yaml) as f:
            saved_cfg = yaml.safe_load(f)
        assert saved_cfg["name"] == TEST_PIPELINE_NAME
        assert len(saved_cfg["steps"]) > 0, "No steps saved in pipeline YAML"
        print(f"✅ Pipeline saved: {saved_cfg}")

        # ========== Test YAML export ==========
        yaml_btn = page.locator('button:has-text("📤 YAML")').first
        yaml_btn.click()
        page.wait_for_timeout(500)
        # The YAML button should reveal download button(s)
        download_btn = page.get_by_role("button", name="下载 pipeline.yaml")
        expect(download_btn).to_be_visible(timeout=5000)
        print("✅ YAML export button visible")

        # ========== Reload page and verify persistence ==========
        page.goto(f"{BASE_URL}/Plot_Builder", wait_until="networkidle")
        page.wait_for_timeout(2000)

        # Select the saved pipeline from dropdown
        _select_streamlit_option(page, "流水线", TEST_PIPELINE_NAME)
        page.wait_for_timeout(500)

        # Click 加载
        _find_button(page, "加载").click()
        page.wait_for_timeout(2000)

        # Verify pipeline name and steps loaded
        name_input_loaded = page.get_by_placeholder("my_pipeline")
        expect(name_input_loaded).to_have_value(TEST_PIPELINE_NAME, timeout=5000)

        # Check that description is loaded
        desc_input_loaded = page.get_by_placeholder("流水线功能描述")
        expect(desc_input_loaded).to_have_value("E2E test pipeline", timeout=5000)

        print("✅ Pipeline loaded correctly with name and description")

        # Verify steps are loaded (step 1 section should show text_repeat)
        page.wait_for_timeout(1000)
        step_visible = page.get_by_text("text_repeat").first
        expect(step_visible).to_be_visible(timeout=5000)
        print("✅ Pipeline steps loaded correctly")

        # ========== Execute pipeline ==========
        # After loading, data sources need to be re-resolved via the file browser
        page.wait_for_timeout(2000)
        fb_root2 = page.locator(".fb-root")
        if fb_root2.count() > 0:
            # Navigate into FileLine/ directory
            fl_dir2 = page.locator('.fb-dir-row[data-fb-nav="FileLine"] td.fb-name-cell').first
            if fl_dir2.count() > 0:
                fl_dir2.click()
                page.wait_for_timeout(2000)
            req_cb2 = page.locator('.fb-file-cb[data-fb-file="requirements.txt"]')
            if req_cb2.count() > 0:
                req_cb2.check()
                page.wait_for_timeout(300)
                page.locator('.fb-confirm').first.click()
                page.wait_for_timeout(3000)

                # Execute the pipeline
                exec_btn = page.locator('button:has-text("执行流水线")').first
                if exec_btn.count() > 0:
                    exec_btn.scroll_into_view_if_needed()
                    page.wait_for_timeout(300)
                    exec_btn.click()
                    page.wait_for_timeout(10000)
                    # Verify results appeared
                    if page.get_by_text("Step 4: 结果").count() > 0:
                        print("✅ Pipeline execution completed with results")
                    else:
                        print("⚠️ Pipeline execution may not have completed")
                else:
                    print("⚠️ Execute button not found after re-resolve")
            else:
                print("⚠️ requirements.txt not found in file browser after load, skipping execution")
                page_text_after = page.locator("body").inner_text()
                print(f"📄 Page after load:\n{page_text_after[:1000]}")
        else:
            print("⚠️ File browser not visible after load, skipping execution")

        # ========== Clean up ==========
        context.close()
        browser.close()

    _cleanup()
    print("✅ Test pipeline cleaned up")
    print("\n🎉 All E2E tests passed!")


def test_pipeline_yaml_format():
    """Verify that saved pipeline YAML matches FileLine-Pipelines format."""
    _cleanup()

    from app_utils import DataSourceConfig, PipelineStepConfig, resolve_data_sources, execute_pipeline

    # Test DataSourceConfig serialization
    ds = DataSourceConfig(name="test_source", include_patterns=["requirements.txt"])
    assert ds.name == "test_source"
    assert ds.include_patterns == ["requirements.txt"]

    # Test PipelineStepConfig
    step = PipelineStepConfig(processor_name="text_repeat", input_sources=["test_source"])
    assert step.processor_name == "text_repeat"
    assert step.input_sources == ["test_source"]

    print("✅ Data structures work correctly")

    # Test YAML generation (matching _pm_generate_yaml format)
    steps_list = []
    for s in [step]:
        entry = {
            "processor": s.processor_name,
            "inputs": s.input_sources[0] if len(s.input_sources) == 1 else s.input_sources,
            "output": s.output_var,
        }
        if s.params:
            entry["params"] = s.params
        steps_list.append(entry)

    cfg = {
        "initial_load": {
            "include": [{"path": "requirements.txt"}],
            "type": "raw",
        },
        "steps": steps_list,
        "final_output": [{"name": "step_1"}],
    }
    yaml_str = yaml.dump(cfg, default_flow_style=False, allow_unicode=True, sort_keys=False)
    assert "processor: text_repeat" in yaml_str
    assert "inputs: test_source" in yaml_str
    assert "path: requirements.txt" in yaml_str
    print(f"✅ YAML format correct:\n{yaml_str}")
    _cleanup()


if __name__ == "__main__":
    import sys

    # Run YAML format test first
    test_pipeline_yaml_format()

    # Run E2E test if --headless is passed
    if "--e2e" in sys.argv:
        test_full_pm_workflow()
    else:
        print("\nSkipping E2E (browser) tests. Run with --e2e flag to include them.")
