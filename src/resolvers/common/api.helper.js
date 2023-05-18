import axios from 'axios'

export default {
  createGetRequest: async (requestUrl, config) => {
    try {
      const response = await axios.get(requestUrl, config)
      const { data, status, statusText } = response
      return {
        data,
        status,
        statusText
      }
    } catch (e) {
      console.log(`--- Error occurred on api request. Error: ${e} --- `)
      return {}
    }
  }
}
