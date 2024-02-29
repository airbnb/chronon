# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information


#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

project = "Chronon"
copyright = "The Chronon Authors"
# author = 'Nikhil Simha'
release = "0.0.60"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.duration",
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.todo",
    "sphinx.ext.coverage",
]

source_parsers = {
    ".md": "recommonmark.parser.CommonMarkParser",
}

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

# -- Options for myst parser --------------------------------------------------
myst_heading_anchors = 3

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_book_theme"

html_theme_options = {
    # "collapse_navigation": True,
    # "home_page_in_toc": True,
    "navbar_align": "right",
    "github_url": "https://github.com/airbnb/chronon",
    "logo": {
        "image_light": "_static/logo_curly.png",
        "image_dark": "_static/logo_curly_white.png",
    },
    "favicons": [
        {
            "rel": "icon",
            "sizes": "64x64",
            "href": "favicon_curly.png",
        }
    ],
    "show_toc_level": 2,
    "use_download_button": True,
    "icon_links": [
        {
            "name": "Discord",
            "url": "https://discord.gg/GbmGATNqqP",
            "icon": "fa-brands fa-discord",
            "type": "fontawesome",
        },
        # {
        #     "name": "Pip Package",
        #     "url": "https://pypi.org/project/chronon-ai/",
        #     "icon": "fa-brands fa-python",
        #     "type": "fontawesome",
        # }
    ],
    "article_header_end": ["navbar-icon-links.html", "theme-switcher.html"],
    # "navbar_end": ["navbar-icon-links.html", "search-field.html"]
}

html_sidebars = {
    "**": ["navbar-logo.html", "search-field.html", "sbt-sidebar-nav.html"]
}
html_title = "Chronon"
html_static_path = ["_static"]
html_css_files = ["chronon.css"]
html_context = {"default_mode": "light"}
html_file_suffix = ".html"
