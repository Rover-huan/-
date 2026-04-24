import {useEffect, useState} from 'react';
import {ApiError, UnauthorizedError, getSession, login, logout, register} from '../../lib/api';
import type {SessionResponse, UserResponse} from '../../lib/types';
import type {AlertState, AuthDraft, AuthMode} from '../types';

const EMAIL_STORAGE_KEY = 'smartanalyst.email';

function readStoredValue(key: string) {
  if (typeof window === 'undefined') {
    return null;
  }
  return window.localStorage.getItem(key);
}

function persistStoredValue(key: string, value: string | null) {
  if (typeof window === 'undefined') {
    return;
  }

  if (value) {
    window.localStorage.setItem(key, value);
  } else {
    window.localStorage.removeItem(key);
  }
}

function getErrorMessage(error: unknown) {
  if (error instanceof ApiError) {
    return error.detail;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return '操作失败，请稍后重试。';
}

export interface SessionController {
  user: UserResponse | null;
  loading: boolean;
  authMode: AuthMode;
  authDraft: AuthDraft;
  authBusy: boolean;
  authAlert: AlertState | null;
  setAuthMode: (mode: AuthMode) => void;
  setAuthField: (field: keyof AuthDraft, value: string) => void;
  submitAuth: () => Promise<void>;
  logoutUser: (message?: string) => Promise<void>;
}

export function useSession(): SessionController {
  const [user, setUser] = useState<UserResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [authMode, setAuthMode] = useState<AuthMode>('login');
  const [authDraft, setAuthDraft] = useState<AuthDraft>({
    email: readStoredValue(EMAIL_STORAGE_KEY) || '',
    password: '',
    captchaVerifyParam: '',
  });
  const [authBusy, setAuthBusy] = useState(false);
  const [authAlert, setAuthAlert] = useState<AlertState | null>(null);

  useEffect(() => {
    persistStoredValue(EMAIL_STORAGE_KEY, authDraft.email.trim().toLowerCase() || null);
  }, [authDraft.email]);

  useEffect(() => {
    let cancelled = false;

    async function bootstrapSession() {
      try {
        const session = await getSession();
        if (cancelled) {
          return;
        }
        setUser(session.user);
      } catch (error) {
        if (cancelled) {
          return;
        }
        if (!(error instanceof UnauthorizedError)) {
          setAuthAlert({tone: 'error', text: getErrorMessage(error)});
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void bootstrapSession();

    return () => {
      cancelled = true;
    };
  }, []);

  function setAuthField(field: keyof AuthDraft, value: string) {
    setAuthDraft((previous) => ({
      ...previous,
      [field]: value,
    }));
  }

  async function submitAuth() {
    setAuthBusy(true);
    setAuthAlert(null);

    const normalizedEmail = authDraft.email.trim().toLowerCase();
    const payload = {
      email: normalizedEmail,
      password: authDraft.password,
      captcha_verify_param: authMode === 'register' ? authDraft.captchaVerifyParam.trim() || null : null,
    };

    try {
      if (authMode === 'register') {
        await register(payload);
        setAuthMode('login');
        setAuthDraft({
          email: normalizedEmail,
          password: '',
          captchaVerifyParam: '',
        });
        setAuthAlert({tone: 'success', text: '注册成功，请使用刚才的邮箱和密码登录。'});
        return;
      }

      const session: SessionResponse = await login(payload);
      setUser(session.user);
      setAuthDraft((previous) => ({
        ...previous,
        email: normalizedEmail,
        password: '',
        captchaVerifyParam: '',
      }));
      setAuthAlert(null);
    } catch (error) {
      setAuthAlert({tone: 'error', text: getErrorMessage(error)});
    } finally {
      setAuthBusy(false);
    }
  }

  async function logoutUser(message?: string) {
    try {
      await logout();
    } catch {
      // Best-effort logout. Local session state should still be cleared.
    }
    setUser(null);
    setAuthMode('login');
    setAuthDraft((previous) => ({
      ...previous,
      password: '',
      captchaVerifyParam: '',
    }));
    setAuthAlert(message ? {tone: 'info', text: message} : null);
  }

  return {
    user,
    loading,
    authMode,
    authDraft,
    authBusy,
    authAlert,
    setAuthMode,
    setAuthField,
    submitAuth,
    logoutUser,
  };
}
