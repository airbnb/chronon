def _twine_upload_impl(ctx):
    wheel_file = ctx.file.wheel
    twine_binary = ctx.executable._twine
    output_file = ctx.actions.declare_file(ctx.label.name + ".log")
    pypirc_file = ctx.file.pypirc
    version = ctx.var.get("version", "0.0.0")  # Default to "0.0.0" if not provided

    # Use the .pypirc file for authentication
    command = "{twine} upload --config-file {pypirc} --repository-url {repo_url} {wheel} # Version: {version}".format(
        twine = twine_binary.path,
        pypirc = pypirc_file.path,
        repo_url = ctx.attr.repository_url,
        wheel = wheel_file.path,
        version = version,  # Include the version in the command
    )
    print(command)

    # Create a script to run the command
    script = ctx.actions.declare_file(ctx.label.name + ".sh")
    ctx.actions.write(
        output = script,
        content = command,
        is_executable = True,
    )

    # Return the executable script and the output file
    return [
        DefaultInfo(
            files = depset([output_file]),
            executable = script,
        ),
    ]

twine_upload = rule(
    implementation = _twine_upload_impl,
    attrs = {
        "wheel": attr.label(allow_single_file = True, mandatory = True),
        "repository_url": attr.string(default = "https://upload.pypi.org/legacy/"),
        "pypirc": attr.label(
            allow_single_file = True,
            default = "//path/to:dot_pypirc",  # Reference the .pypirc file
        ),
        "version": attr.string(mandatory = True),  # Add token attribute
        "_twine": attr.label(
            default = Label("//tools/build_rules/python:twine_main"),  # Reference the twine executable
            executable = True,
            cfg = "exec",
        ),
    },
    executable = True,  # Mark the rule as executable
)
