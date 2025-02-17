def _twine_upload_impl(ctx):
    wheel_file = ctx.file.wheel
    twine_binary = ctx.executable._twine
    version = ctx.var.get("version", "0.0.0")

    # Create the command
    command = "#!/bin/bash\n"
    command += "{twine} upload --repository-url {repo_url} {wheel} # Version: {version}".format(
        twine = twine_binary.short_path,
        repo_url = ctx.attr.repository_url,
        wheel = wheel_file.short_path,
        version = version,
    )

    # Create a wrapper script
    script = ctx.actions.declare_file(ctx.label.name + ".sh")
    ctx.actions.write(
        output = script,
        content = command,
        is_executable = True,
    )

    # Create runfiles with all necessary dependencies
    runfiles = ctx.runfiles(
        files = [
            wheel_file,
            twine_binary,
        ],
    )

    return [
        DefaultInfo(
            executable = script,
            runfiles = runfiles,
        ),
    ]

twine_upload = rule(
    implementation = _twine_upload_impl,
    attrs = {
        "wheel": attr.label(allow_single_file = True, mandatory = True),
        "repository_url": attr.string(default = "https://upload.pypi.org/legacy/"),
        "version": attr.string(mandatory = True),
        "_twine": attr.label(
            default = Label("//tools/build_rules/python:twine_main"),
            executable = True,
            cfg = "exec",
        ),
    },
    executable = True,
)
