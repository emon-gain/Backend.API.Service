const statusPrefix = {
  400: 'BadRequest',
  408: 'Time-out',
  404: 'Not Found',
  500: 'ServerError'
}

export const createErrorResponse = (statusCode, message, err) => {
  const prefix = statusPrefix[statusCode] || statusPrefix[500]
  let response = `${prefix} ${message}`
  if (err) {
    response += `\n${err.stack}`
  }
  console.log(response)
  return response
}
