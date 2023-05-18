export default (date) => {
  const isoDate = date.toISOString() ? date.toISOString() : null
  return isoDate
}
