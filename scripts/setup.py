import runpy


def main():
    runpy.run_module("snisb_dam_collector", run_name="__main__")
    runpy.run_module("risk_collector", run_name="__main__")
    runpy.run_module("cemaden_collector", run_name="__main__")
    runpy.run_module("snisb_dam_collector", run_name="__main__")
    runpy.run_module("ibge_download_script", run_name="__main__")
    # runpy.run_module("iphan", run_name="__main__")


if __name__ == "__main__":
    main()
