const CACHE_NAME = 'elim-app-cache-v3.8';
const ASSETS_TO_CACHE = [
  './',
  './index.html',
  './css/style.css',
  './js/app.js',
  './manifest.json',
  './assets/icons/icon-192x192.png',
  './assets/icons/icon-512x512.png'
];

// Escuchar mensajes para forzar actualización (SKIP_WAITING)
self.addEventListener('message', event => {
  if (event.data && event.data.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});

// Instalación: Cachear assets básicos
self.addEventListener('install', event => {
  // Ya no llamamos a self.skipWaiting() aquí para permitir que el usuario decida cuándo actualizar
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => cache.addAll(ASSETS_TO_CACHE))
  );
});

// Activación: Limpiar caches antiguas
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(cacheNames => {
      return Promise.all(
        cacheNames.map(cacheName => {
          if (cacheName !== CACHE_NAME) {
            console.log('Borrando cache antiguo:', cacheName);
            return caches.delete(cacheName);
          }
        })
      );
    })
  );
});

// Estrategia: Network First para archivos vitales, Cache First para el resto
self.addEventListener('fetch', event => {
  const url = new URL(event.request.url);
  
  // Para la app principal, scripts y estilos, intentamos siempre Red primero para evitar versiones viejas en móviles
  const isVitalAsset = url.pathname.endsWith('.js') || 
                       url.pathname.endsWith('.html') || 
                       url.pathname.endsWith('.css') ||
                       url.pathname === '/';

  if (isVitalAsset) {
    event.respondWith(
      fetch(event.request)
        .then(response => {
          const clonedResponse = response.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(event.request, clonedResponse));
          return response;
        })
        .catch(() => caches.match(event.request))
    );
  } else {
    // Para el resto (imágenes, etc), Cache First
    event.respondWith(
      caches.match(event.request)
        .then(response => response || fetch(event.request))
    );
  }
});
