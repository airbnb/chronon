# TEMPLATE-INCLUDE-BEGIN
# TEMPLATE-INCLUDE-END

def java_info_in_target(target):

# TEMPLATE-INCLUDE-BEGIN
  return JavaInfo in target
# TEMPLATE-INCLUDE-END

def get_java_info(target):

# TEMPLATE-INCLUDE-BEGIN
  if JavaInfo in target:
      return target[JavaInfo]
  else:
      return None
# TEMPLATE-INCLUDE-END

def java_info_reference():

# TEMPLATE-INCLUDE-BEGIN
  return [JavaInfo]
# TEMPLATE-INCLUDE-END
