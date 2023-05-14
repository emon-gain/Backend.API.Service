# Use an official Node.js runtime as the base image
FROM node:14

# Set the working directory in the container
WORKDIR /app

# Copy package.json and package-lock.json to the container
COPY package*.json ./

# Install the application dependencies
RUN npm install
RUN npm install bcrypt

# Copy the application source code to the container
COPY . .

# Expose port 3000 for the application
EXPOSE 4000

# Define the command to run the application in development mode
CMD [ "npm", "run", "dev" ]
