// jest.config.js
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  testMatch: ["**/test/**/*.test.ts"], // Looks for test files in a 'test' directory
  moduleFileExtensions: ['ts', 'js', 'json', 'node'],
  verbose: true,
  collectCoverage: true, // Optional: Collects test coverage
  coverageDirectory: 'coverage', // Optional: Directory for coverage reports
  collectCoverageFrom: [
    "src/**/*.{ts,js}", // Optional: Collect coverage from your source files
    "!src/**/*.d.ts", // Exclude declaration files
  ],
  globals: {
    'ts-jest': {
      tsconfig: 'tsconfig.json',
    },
  },
};