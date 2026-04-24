import type {
  AdminAccountResponse,
  AdminDashboardResponse,
  AdminJobDetailResponse,
  AdminJobsResponse,
  AdminSessionResponse,
  AdminUsersResponse,
  AdminUserUsageResponse,
  JobDeleteResponse,
  JobArtifactListResponse,
  JobEventListResponse,
  JobListResponse,
  JobSummaryResponse,
  JobTaskListResponse,
  SessionResponse,
  UserResponse,
} from './types';

function getDefaultApiBaseUrl() {
  if (typeof window === 'undefined') {
    return import.meta.env.DEV ? 'http://127.0.0.1:8000' : '/api';
  }

  const {hostname, protocol} = window.location;
  if (import.meta.env.DEV) {
    if (hostname === 'localhost') {
      return `${protocol}//localhost:8000`;
    }
    if (hostname === '127.0.0.1') {
      return `${protocol}//127.0.0.1:8000`;
    }
    return `${protocol}//${hostname}:8000`;
  }

  return '/api';
}

const RAW_API_BASE_URL =
  (import.meta.env.VITE_API_BASE_URL as string | undefined)?.trim() || getDefaultApiBaseUrl();
export const API_BASE_URL = RAW_API_BASE_URL.replace(/\/$/, '');

export class ApiError extends Error {
  status: number;
  detail: string;

  constructor(status: number, detail: string) {
    super(detail);
    this.name = 'ApiError';
    this.status = status;
    this.detail = detail;
  }
}

export class UnauthorizedError extends ApiError {
  constructor(detail = '登录状态已失效，请重新登录。') {
    super(401, detail);
    this.name = 'UnauthorizedError';
  }
}

interface AuthPayload {
  email: string;
  password: string;
  captcha_verify_param?: string | null;
}

interface EmailVerificationPayload {
  email: string;
  token: string;
}

interface SelectionPayload {
  selected_task_ids: number[];
}

interface BinaryResponse {
  blob: Blob;
  filename: string | null;
}

function resolveApiUrl(pathOrUrl: string): string {
  if (/^https?:\/\//i.test(pathOrUrl)) {
    return pathOrUrl;
  }
  if (API_BASE_URL) {
    if (/^https?:\/\//i.test(API_BASE_URL)) {
      return new URL(pathOrUrl, API_BASE_URL).toString();
    }
    const base = API_BASE_URL.startsWith('/') ? API_BASE_URL : `/${API_BASE_URL}`;
    const path = pathOrUrl.startsWith('/') ? pathOrUrl : `/${pathOrUrl}`;
    return `${base}${path}`;
  }
  return pathOrUrl;
}

async function buildApiError(response: Response): Promise<ApiError> {
  let detail = response.statusText || '请求失败，请稍后重试。';
  const contentType = response.headers.get('content-type') || '';

  if (contentType.includes('application/json')) {
    const payload = (await response.json()) as {detail?: unknown};
    const detailValue = payload.detail;
    if (typeof detailValue === 'string' && detailValue.trim()) {
      detail = detailValue.trim();
    } else if (Array.isArray(detailValue)) {
      detail = detailValue
        .map((item) => {
          if (!item || typeof item !== 'object') {
            return '';
          }
          const record = item as {loc?: unknown[]; msg?: unknown};
          const location = Array.isArray(record.loc) ? record.loc.join('.') : 'request';
          const message = typeof record.msg === 'string' ? record.msg : '参数错误';
          return `${location}: ${message}`;
        })
        .filter(Boolean)
        .join('；');
    }
  } else {
    const text = await response.text();
    if (text.trim()) {
      detail = text.trim();
    }
  }

  if (response.status === 401) {
    return new UnauthorizedError(detail);
  }

  return new ApiError(response.status, detail);
}

async function requestJson<T>(pathOrUrl: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  if (init.body && !(init.body instanceof FormData) && !headers.has('Content-Type')) {
    headers.set('Content-Type', 'application/json');
  }

  const response = await fetch(resolveApiUrl(pathOrUrl), {
    ...init,
    headers,
    credentials: 'include',
  });

  if (!response.ok) {
    throw await buildApiError(response);
  }

  return (await response.json()) as T;
}

function getFilenameFromHeaders(response: Response): string | null {
  const disposition = response.headers.get('content-disposition');
  if (!disposition) {
    return null;
  }

  const utf8Match = disposition.match(/filename\*=UTF-8''([^;]+)/i);
  if (utf8Match?.[1]) {
    try {
      return decodeURIComponent(utf8Match[1]);
    } catch {
      return utf8Match[1];
    }
  }

  const plainMatch = disposition.match(/filename="?([^"]+)"?/i);
  return plainMatch?.[1] ?? null;
}

export async function fetchBinary(pathOrUrl: string): Promise<BinaryResponse> {
  const response = await fetch(resolveApiUrl(pathOrUrl), {
    credentials: 'include',
  });

  if (!response.ok) {
    throw await buildApiError(response);
  }

  return {
    blob: await response.blob(),
    filename: getFilenameFromHeaders(response),
  };
}

export function openJobStream(pathOrUrl: string): EventSource {
  return new EventSource(resolveApiUrl(pathOrUrl), {withCredentials: true});
}

export function register(payload: AuthPayload): Promise<UserResponse> {
  return requestJson<UserResponse>('/auth/register', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export function login(payload: AuthPayload): Promise<SessionResponse> {
  return requestJson<SessionResponse>('/auth/login', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export function getSession(): Promise<SessionResponse> {
  return requestJson<SessionResponse>('/me');
}

export function logout(): Promise<{status: string}> {
  return requestJson<{status: string}>('/auth/logout', {method: 'POST'});
}

export function verifyEmail(payload: EmailVerificationPayload): Promise<{status: string; message: string}> {
  return requestJson<{status: string; message: string}>('/auth/verify-email', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export function resendVerification(email: string): Promise<{status: string; message: string}> {
  return requestJson<{status: string; message: string}>('/auth/resend-verification', {
    method: 'POST',
    body: JSON.stringify({email}),
  });
}

export function getJobs(limit = 12): Promise<JobListResponse> {
  return requestJson<JobListResponse>(`/jobs?limit=${limit}`);
}

export function createJob(files: File[]): Promise<JobSummaryResponse> {
  const formData = new FormData();
  for (const file of files) {
    formData.append('files', file, file.name);
  }

  return requestJson<JobSummaryResponse>('/jobs', {
    method: 'POST',
    body: formData,
  });
}

export function deleteJob(jobId: string): Promise<JobDeleteResponse> {
  return requestJson<JobDeleteResponse>(`/jobs/${jobId}`, {
    method: 'DELETE',
  });
}

export function getJobSummary(jobId: string): Promise<JobSummaryResponse> {
  return requestJson<JobSummaryResponse>(`/jobs/${jobId}`);
}

export function getJobTasks(jobId: string): Promise<JobTaskListResponse> {
  return requestJson<JobTaskListResponse>(`/jobs/${jobId}/tasks`);
}

export function getJobEvents(jobId: string, after?: string | null): Promise<JobEventListResponse> {
  const suffix = after ? `?after=${encodeURIComponent(after)}` : '';
  return requestJson<JobEventListResponse>(`/jobs/${jobId}/events${suffix}`);
}

export function getJobArtifacts(jobId: string): Promise<JobArtifactListResponse> {
  return requestJson<JobArtifactListResponse>(`/jobs/${jobId}/artifacts`);
}

export function submitSelection(
  jobId: string,
  selectedTaskIds: number[],
): Promise<JobSummaryResponse> {
  const payload: SelectionPayload = {
    selected_task_ids: selectedTaskIds,
  };

  return requestJson<JobSummaryResponse>(`/jobs/${jobId}/selection`, {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export function sendPresenceHeartbeat(payload: {
  current_job_id?: string | null;
  current_path?: string | null;
}): Promise<{status: string; message: string}> {
  return requestJson<{status: string; message: string}>('/presence/heartbeat', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export function adminLogin(payload: {email: string; password: string}): Promise<AdminSessionResponse> {
  return requestJson<AdminSessionResponse>('/admin/auth/login', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export function adminLogout(): Promise<{status: string; message: string}> {
  return requestJson<{status: string; message: string}>('/admin/auth/logout', {method: 'POST'});
}

export function getAdminSession(): Promise<AdminSessionResponse> {
  return requestJson<AdminSessionResponse>('/admin/me');
}

export function getAdminDashboard(): Promise<AdminDashboardResponse> {
  return requestJson<AdminDashboardResponse>('/admin/dashboard');
}

export function getAdminJobs(params: {
  status?: string;
  email?: string;
  failedOnly?: boolean;
  limit?: number;
} = {}): Promise<AdminJobsResponse> {
  const search = new URLSearchParams();
  if (params.status) {
    search.set('status', params.status);
  }
  if (params.email) {
    search.set('email', params.email);
  }
  if (params.failedOnly) {
    search.set('failed_only', 'true');
  }
  search.set('limit', String(params.limit ?? 50));
  return requestJson<AdminJobsResponse>(`/admin/jobs?${search.toString()}`);
}

export function getAdminJobDetail(jobId: string): Promise<AdminJobDetailResponse> {
  return requestJson<AdminJobDetailResponse>(`/admin/jobs/${jobId}`);
}

export function retryAdminJob(jobId: string): Promise<{status: string; message: string}> {
  return requestJson<{status: string; message: string}>(`/admin/jobs/${jobId}/retry`, {method: 'POST'});
}

export function cancelAdminJob(jobId: string): Promise<{status: string; message: string}> {
  return requestJson<{status: string; message: string}>(`/admin/jobs/${jobId}/cancel`, {method: 'POST'});
}

export function getAdminUsers(params: {
  email?: string;
  onlineOnly?: boolean;
  limit?: number;
} = {}): Promise<AdminUsersResponse> {
  const search = new URLSearchParams();
  if (params.email) {
    search.set('email', params.email);
  }
  if (params.onlineOnly) {
    search.set('online_only', 'true');
  }
  search.set('limit', String(params.limit ?? 50));
  return requestJson<AdminUsersResponse>(`/admin/users?${search.toString()}`);
}

export function getAdminUserUsage(userLookup: string): Promise<AdminUserUsageResponse> {
  return requestJson<AdminUserUsageResponse>(`/admin/users/${encodeURIComponent(userLookup)}/usage`);
}

export function grantViewerAdmin(email: string): Promise<AdminAccountResponse> {
  return requestJson<AdminAccountResponse>('/admin/admins/grant', {
    method: 'POST',
    body: JSON.stringify({email, role: 'viewer'}),
  });
}

export function updateAdminUserStatus(
  userLookup: string,
  nextStatus: 'active' | 'disabled',
): Promise<UserResponse> {
  return requestJson<UserResponse>(`/admin/users/${encodeURIComponent(userLookup)}/status`, {
    method: 'PATCH',
    body: JSON.stringify({status: nextStatus}),
  });
}

export function updateAdminUserQuota(
  userLookup: string,
  payload: {
    daily_job_limit: number | null;
    daily_upload_bytes_limit: number | null;
    active_job_limit: number | null;
  },
): Promise<AdminUserUsageResponse> {
  return requestJson<AdminUserUsageResponse>(`/admin/users/${encodeURIComponent(userLookup)}/quota`, {
    method: 'PUT',
    body: JSON.stringify(payload),
  });
}

export function resendAdminUserVerification(userLookup: string): Promise<{status: string; message: string}> {
  return requestJson<{status: string; message: string}>(
    `/admin/users/${encodeURIComponent(userLookup)}/resend-verification`,
    {method: 'POST'},
  );
}
