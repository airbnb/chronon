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

project = 'Chronon'
copyright = 'The Chronon Authors'
# author = 'Nikhil Simha'
release = '0.0.60'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.duration',
    'myst_parser',
    'sphinx.ext.autodoc',
    'sphinx.ext.todo',
    'sphinx.ext.coverage',
]

source_parsers = {
    '.md': 'recommonmark.parser.CommonMarkParser',
}

source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

#-- Options for myst parser --------------------------------------------------
myst_heading_anchors = 3

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_book_theme"

html_theme_options = {
    # "collapse_navigation": True,
    "home_page_in_toc": True,
    "navbar_align": "right",
    "github_url": "https://github.com/airbnb/chronon",
    "logo": {
      "image_light": "logo_curly.png",
      "image_dark": "logo_curly.png",
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
            "url": "https://discord.gg/8fSAfXt9",
            "icon": "fa-brands fa-discord",
            "type": "fontawesome",
        },
        # <i class="fa-brands fa-python"></i>
        {
            "name": "Pip Package",
            "url": "https://pypi.org/project/chronon-ai/",
            "icon": "fa-brands fa-python",
            "type": "fontawesome",
        }
        # {
        #     "name": "Twitter",
        #     "url": "https://twitter.com/<your-handle>",
        #     "icon": "fa-brands fa-square-twitter",
        #     # The default for `type` is `fontawesome`, so it is not required in the above examples
        # },
        # {
        #     "name": "Mastodon",
        #     "url": "https://<your-host>@<your-handle>",
        #     "icon": "fa-brands fa-mastodon",
        # },
    ],
    # "navbar_end": ["navbar-icon-links.html", "search-field.html"]
}

# html_sidebars = {
#     "**": ["navbar-logo.html", "search-field.html", "sbt-sidebar-nav.html"]
# }
html_title = "Chronon"
html_static_path = ['_static']
html_css_files = ["chronon.css"]
html_context = {"default_mode": "light"}
# html_use_modindex = True
# html_copy_source = True
# html_domain_indices = False
html_file_suffix = '.html'


""" ==================== BIG =================
-pst-header-height: 4rem;
    --pst-header-article-height: calc(var(--pst-header-height)*2/3);
    --pst-sidebar-secondary: 17rem;
    --pst-font-size-base: 1rem;
    --pst-font-size-h1: 2.5rem;
    --pst-font-size-h2: 2rem;
    --pst-font-size-h3: 1.75rem;
    --pst-font-size-h4: 1.5rem;
    --pst-font-size-h5: 1.25rem;
    --pst-font-size-h6: 1.1rem;
    --pst-font-size-milli: 0.9rem;
    --pst-sidebar-font-size: 0.9rem;
    --pst-sidebar-font-size-mobile: 1.1rem;
    --pst-sidebar-header-font-size: 1.2rem;
    --pst-sidebar-header-font-weight: 600;
    --pst-admonition-font-weight-heading: 600;
    --pst-font-weight-caption: 300;
    --pst-font-weight-heading: 400;
    --pst-font-family-base-system: -apple-system,BlinkMacSystemFont,Segoe UI,"Helvetica Neue",Arial,sans-serif,Apple Color Emoji,Segoe UI Emoji,Segoe UI Symbol;
    --pst-font-family-monospace-system: "SFMono-Regular",Menlo,Consolas,Monaco,Liberation Mono,Lucida Console,monospace;
    --pst-font-family-base: var(--pst-font-family-base-system);
    --pst-font-family-heading: var(--pst-font-family-base-system);
    --pst-font-family-monospace: var(--pst-font-family-monospace-system);
    --pst-font-size-icon: 1.5rem;
    --pst-icon-check-circle: "";
    --pst-icon-info-circle: "";
    --pst-icon-exclamation-triangle: "";
    --pst-icon-exclamation-circle: "";
    --pst-icon-times-circle: "";
    --pst-icon-lightbulb: "";
    --pst-icon-download: "";
    --pst-icon-angle-left: "";
    --pst-icon-angle-right: "";
    --pst-icon-external-link: "";
    --pst-icon-search-minus: "";
    --pst-icon-github: "";
    --pst-icon-gitlab: "";
    --pst-icon-share: "";
    --pst-icon-bell: "";
    --pst-icon-pencil: "";
    --pst-breadcrumb-divider: "";
    --pst-icon-admonition-default: var(--pst-icon-bell);
    --pst-icon-admonition-note: var(--pst-icon-info-circle);
    --pst-icon-admonition-attention: var(--pst-icon-exclamation-circle);
    --pst-icon-admonition-caution: var(--pst-icon-exclamation-triangle);
    --pst-icon-admonition-warning: var(--pst-icon-exclamation-triangle);
    --pst-icon-admonition-danger: var(--pst-icon-exclamation-triangle);
    --pst-icon-admonition-error: var(--pst-icon-times-circle);
    --pst-icon-admonition-hint: var(--pst-icon-lightbulb);
    --pst-icon-admonition-tip: var(--pst-icon-lightbulb);
    --pst-icon-admonition-important: var(--pst-icon-exclamation-circle);
    --pst-icon-admonition-seealso: var(--pst-icon-share);
    --pst-icon-admonition-todo: var(--pst-icon-pencil);
    --pst-icon-versionmodified-default: var(--pst-icon-exclamation-circle);
    --pst-icon-versionmodified-added: var(--pst-icon-exclamation-circle);
    --pst-icon-versionmodified-changed: var(--pst-icon-exclamation-circle);
    --pst-icon-versionmodified-deprecated: var(--pst-icon-exclamation-circle);
    font-size: var(--pst-font-size-base);
"""


""" ==== SMALL ====
--pst-header-height: 4rem;
    --pst-header-article-height: calc(var(--pst-header-height)*2/3);
    --pst-sidebar-secondary: 17rem;
    --pst-font-size-base: 1rem;
    --pst-font-size-h1: 2.5rem;
    --pst-font-size-h2: 2rem;
    --pst-font-size-h3: 1.75rem;
    --pst-font-size-h4: 1.5rem;
    --pst-font-size-h5: 1.25rem;
    --pst-font-size-h6: 1.1rem;
    --pst-font-size-milli: 0.9rem;
    --pst-sidebar-font-size: 0.9rem;
    --pst-sidebar-font-size-mobile: 1.1rem;
    --pst-sidebar-header-font-size: 1.2rem;
    --pst-sidebar-header-font-weight: 600;
    --pst-admonition-font-weight-heading: 600;
    --pst-font-weight-caption: 300;
    --pst-font-weight-heading: 400;
    --pst-font-family-base-system: -apple-system,BlinkMacSystemFont,Segoe UI,"Helvetica Neue",Arial,sans-serif,Apple Color Emoji,Segoe UI Emoji,Segoe UI Symbol;
    --pst-font-family-monospace-system: "SFMono-Regular",Menlo,Consolas,Monaco,Liberation Mono,Lucida Console,monospace;
    --pst-font-family-base: var(--pst-font-family-base-system);
    --pst-font-family-heading: var(--pst-font-family-base-system);
    --pst-font-family-monospace: var(--pst-font-family-monospace-system);
    --pst-font-size-icon: 1.5rem;
    --pst-icon-check-circle: "";
    --pst-icon-info-circle: "";
    --pst-icon-exclamation-triangle: "";
    --pst-icon-exclamation-circle: "";
    --pst-icon-times-circle: "";
    --pst-icon-lightbulb: "";
    --pst-icon-download: "";
    --pst-icon-angle-left: "";
    --pst-icon-angle-right: "";
    --pst-icon-external-link: "";
    --pst-icon-search-minus: "";
    --pst-icon-github: "";
    --pst-icon-gitlab: "";
    --pst-icon-share: "";
    --pst-icon-bell: "";
    --pst-icon-pencil: "";
    --pst-breadcrumb-divider: "";
    --pst-icon-admonition-default: var(--pst-icon-bell);
    --pst-icon-admonition-note: var(--pst-icon-info-circle);
    --pst-icon-admonition-attention: var(--pst-icon-exclamation-circle);
    --pst-icon-admonition-caution: var(--pst-icon-exclamation-triangle);
    --pst-icon-admonition-warning: var(--pst-icon-exclamation-triangle);
    --pst-icon-admonition-danger: var(--pst-icon-exclamation-triangle);
    --pst-icon-admonition-error: var(--pst-icon-times-circle);
    --pst-icon-admonition-hint: var(--pst-icon-lightbulb);
    --pst-icon-admonition-tip: var(--pst-icon-lightbulb);
    --pst-icon-admonition-important: var(--pst-icon-exclamation-circle);
    --pst-icon-admonition-seealso: var(--pst-icon-share);
    --pst-icon-admonition-todo: var(--pst-icon-pencil);
    --pst-icon-versionmodified-default: var(--pst-icon-exclamation-circle);
    --pst-icon-versionmodified-added: var(--pst-icon-exclamation-circle);
    --pst-icon-versionmodified-changed: var(--pst-icon-exclamation-circle);
    --pst-icon-versionmodified-deprecated: var(--pst-icon-exclamation-circle);
    font-size: var(--pst-font-size-base);
    scroll-padding-top: calc(var(--pst-header-height) + 1rem)
"""