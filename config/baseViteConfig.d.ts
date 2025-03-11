import { ConfigEnv, UserConfig } from 'vite';
export declare function resolve(path: string): string;
export declare function getBaseConfig(configEnv: ConfigEnv, addtionalScss?: string, deployUrl?: string, viteDeploy?: (url: string) => void): UserConfig;
