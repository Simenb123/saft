import importlib


def test_import_parsers_package():
    parsers = importlib.import_module("parsers")
    assert hasattr(parsers, "__all__")


def test_import_gui_wrapper():
    gui = importlib.import_module("saft_pro_gui")
    assert hasattr(gui, "main")


def test_import_ui_main():
    ui = importlib.import_module("ui_main")
    assert hasattr(ui, "main")
