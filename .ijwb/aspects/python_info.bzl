# TEMPLATE-INCLUDE-BEGIN
load("@rules_python//python:defs.bzl", RulesPyInfo = "PyInfo")
# TEMPLATE-INCLUDE-END

def py_info_in_target(target):

# TEMPLATE-INCLUDE-BEGIN
  if RulesPyInfo in target:
    return True
  if PyInfo in target:
    return True
  return False
# TEMPLATE-INCLUDE-END

def get_py_info(target):

# TEMPLATE-INCLUDE-BEGIN
  if RulesPyInfo in target:
    return target[RulesPyInfo]
  if PyInfo in target:
    return target[PyInfo]
  return None
# TEMPLATE-INCLUDE-END

