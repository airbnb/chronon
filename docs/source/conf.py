# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Chronon'
copyright = '2022, Airbnb'
author = 'Airbnb'
release = '0.0.22'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.duration',
    'myst_parser',
    'sphinx.ext.autodoc',
    'sphinx.ext.todo',
    'sphinx.ext.coverage',
    'sphinx_design',
]

source_parsers = {
    '.md': 'recommonmark.parser.CommonMarkParser',
}

source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'pydata_sphinx_theme'
html_theme_options = {
    "collapse_navigation": True,
    "github_url": "https://github.com/airbnb/chronon",
    "logo": {
      "image_light": "logo.png",
      "image_dark": "logo.png",  # TODO: add different dark version
    },
    "favicons": [
        {
            "rel": "icon",
            "sizes": "64x64",
            "href": "favicon_64_light_bg.png",
        }
    ],
    "navbar_align": "right"
}

html_title = "Feature Framework"
html_static_path = ['_static']
html_css_files = ["chronon.css"]
html_context = {"default_mode": "light"}
html_use_modindex = True
html_copy_source = False
html_domain_indices = False
html_file_suffix = '.html'
