extensions = [
    "sphinx.ext.autodoc",
]

autodoc_member_order = "bysource"
autodoc_default_flags = ["members", "undoc-members", "inherited-members"]

master_doc = "index"
project = u"Gambit"
copyright = u"1975"
author = u"Justin Ross"

version = u"0.1.0"
release = u""

pygments_style = "sphinx"
html_theme = "nature"

html_theme_options = {
    "nosidebar": True,
}
