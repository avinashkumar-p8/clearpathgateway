import axios, { AxiosResponse, AxiosRequestConfig } from 'axios';

export class HttpTestHelper {
  private baseURL: string;

  constructor(baseURL: string = 'http://localhost:8080') {
    this.baseURL = baseURL;
  }

  async get(endpoint: string, config?: AxiosRequestConfig): Promise<AxiosResponse> {
    try {
      return await axios.get(`${this.baseURL}${endpoint}`, config);
    } catch (error) {
      if (axios.isAxiosError(error)) {
        return error.response!;
      }
      throw error;
    }
  }

  async post(endpoint: string, data?: any, config?: AxiosRequestConfig): Promise<AxiosResponse> {
    try {
      return await axios.post(`${this.baseURL}${endpoint}`, data, config);
    } catch (error) {
      if (axios.isAxiosError(error)) {
        return error.response!;
      }
      throw error;
    }
  }

  async put(endpoint: string, data?: any, config?: AxiosRequestConfig): Promise<AxiosResponse> {
    try {
      return await axios.put(`${this.baseURL}${endpoint}`, data, config);
    } catch (error) {
      if (axios.isAxiosError(error)) {
        return error.response!;
      }
      throw error;
    }
  }

  async delete(endpoint: string, config?: AxiosRequestConfig): Promise<AxiosResponse> {
    try {
      return await axios.delete(`${this.baseURL}${endpoint}`, config);
    } catch (error) {
      if (axios.isAxiosError(error)) {
        return error.response!;
      }
      throw error;
    }
  }

  async healthCheck(): Promise<AxiosResponse> {
    return this.get('/api/v1/health/status');
  }

  async getServiceInfo(): Promise<AxiosResponse> {
    return this.get('/api/v1/health/info');
  }
}

export const httpHelper = new HttpTestHelper();
