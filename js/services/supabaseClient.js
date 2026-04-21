// js/services/supabaseClient.js

// TODO: Reemplazar estas variables con las credenciales de tu proyecto Supabase
// Puedes encontrarlas en tu Dashboard de Supabase: Project Settings -> API
const SUPABASE_URL = 'https://cpcfcrkxsvalxrojrskk.supabase.co';
const SUPABASE_ANON_KEY = 'sb_publishable_f2286KjYthUr3oeZCPUkEQ_KJS_KVV9';

// Inicializar y exportar el cliente al objeto window para poder usarlo en toda la app
if (window.supabase) {
    window.supabaseClient = window.supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
    console.log('[Supabase] Cliente inicializado correctamente.');
} else {
    console.error('[Supabase] La librería de Supabase no cargó correctamente desde el CDN.');
}
