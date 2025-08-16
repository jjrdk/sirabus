import os
import xml.etree.ElementTree as ET
import re

REPORTS_DIR = os.path.join(os.path.dirname(__file__), "reports")
OUTPUT_MD = os.path.join(os.path.dirname(__file__), "testreports.md")


def extract_scenarios_from_junit(xml_path):
    scenarios = []
    tree = ET.parse(xml_path)
    root = tree.getroot()
    for testcase in root.findall(".//testcase"):
        scenario_name = testcase.attrib.get("name", "Unnamed Scenario")
        system_out = testcase.find("system-out")
        if system_out is not None and system_out.text:
            # Extract steps between @scenario.begin and @scenario.end
            match = re.search(
                r"@scenario.begin(.*?)@scenario.end", system_out.text, re.DOTALL
            )
            if match:
                steps_block = match.group(1)
                # Extract each step (lines starting with whitespace and a word)
                steps = [
                    line.strip()
                    for line in steps_block.splitlines()
                    if line.strip()
                    and not line.strip().startswith("Scenario Template:")
                ]
            else:
                steps = []
        else:
            steps = []
        scenarios.append({"name": scenario_name, "steps": steps})
    return scenarios


def main():
    all_scenarios = []
    for fname in os.listdir(REPORTS_DIR):
        if fname.startswith("TESTS-") and fname.endswith(".xml"):
            xml_path = os.path.join(REPORTS_DIR, fname)
            scenarios = extract_scenarios_from_junit(xml_path)
            all_scenarios.extend(scenarios)
    with open(OUTPUT_MD, "w", encoding="utf-8") as f:
        f.write("# Behave Test Report\n\n")
        for i, scenario in enumerate(all_scenarios, 1):
            f.write(f"## Scenario {i}: {scenario['name']}\n\n")
            if scenario["steps"]:
                for step in scenario["steps"]:
                    f.write(f"- {step}\n")
            else:
                f.write("_No steps found._\n")
            f.write("\n")


if __name__ == "__main__":
    main()
