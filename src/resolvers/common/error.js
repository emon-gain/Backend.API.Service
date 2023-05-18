export class CustomError extends Error {
  constructor(statusCode, message) {
    super()
    this.statusCode = statusCode
    this.message = message
  }
}

export const handleError = (err, res) => {
  const { statusCode = 500, message } = err
  let status = statusCode
  if (err.name === 'ValidationError') {
    status = 400
  }
  res.status(status).json({
    success: false,
    status,
    message
  })
}
