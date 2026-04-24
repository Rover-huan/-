export type NoticeTone = 'info' | 'success' | 'error';

export interface AlertState {
  tone: NoticeTone;
  text: string;
}

export type AuthMode = 'login' | 'register';

export interface AuthDraft {
  email: string;
  password: string;
  captchaVerifyParam: string;
}
