// js/app.js

const App = {
    state: {
        user: null,        // Perfil completo del usuario
        session: null,     // Sesión de Supabase
        loading: false,
        activeMode: null,  // 'coordinador' | 'discipulador'
        isOnline: navigator.onLine,
        isRetrying: false,
        historyLimit: 50,
        isInitializing: false,
        cache: {
            metadata: null,
            lastFetch: 0,
            views: {}
        }
    },

    async getGlobalMeta(forceRefresh = false) {
        if (!forceRefresh && this.state.cache.metadata && (Date.now() - this.state.cache.lastFetch < 300000)) {
            return this.state.cache.metadata;
        }

        // Evitar múltiples peticiones paralelas durante el arranque (Resiliencia de Red)
        if (this._metaPromise) return this._metaPromise;

        this._metaPromise = (async () => {
            try {
                const RESULTS_META = await Promise.all([
                    window.supabaseClient.from('sectors').select('id, name, coordinator_id, coordinator:profiles!sectors_coordinator_id_fkey(name)').order('name'),
                    window.supabaseClient.from('profiles').select('id, name, sector_id, role, additional_roles, sector:sectors!profiles_sector_id_fkey(name)').order('name'),
                    window.supabaseClient.from('people_assigned').select('id, name, assigned_to').order('name'),
                    window.supabaseClient.from('free_weeks').select('saturday_date, description')
                ]);

                if (RESULTS_META.some(r => r.error && r.error.code !== 'PGRST116' && r.error.code !== '42P01')) {
                    throw new Error('Error al cargar metadatos');
                }

                this.state.cache.metadata = {
                    sectors: RESULTS_META[0].data || [],
                    profiles: RESULTS_META[1].data || [],
                    discipliners: (RESULTS_META[1].data || []).filter(p => ['discipulador', 'pastor', 'coordinador'].includes(p.role)),
                    sheep: RESULTS_META[2].data || [],
                    freeWeeks: RESULTS_META[3]?.data || []
                };
                this.state.cache.lastFetch = Date.now();
                return this.state.cache.metadata;
            } catch (err) {
                console.warn("Fallo metadatos. Usando local o vacio", err);
                return this.state.cache.metadata || { sectors: [], profiles: [], discipliners: [], sheep: [] };
            } finally {
                this._metaPromise = null;
            }
        })();

        return this._metaPromise;
    },

    // --- SISTEMA DE RESILIENCIA Y RENDIMIENTO ---

    // Ejecución segura de promesas con reintento automático (Tolerancia a fallos)
    async safeCall(fn, options = { retries: 1, delay: 500, silent: false }) {
        let lastError;
        for (let i = 0; i < options.retries; i++) {
            try {
                if (!navigator.onLine) throw new Error('Sin conexión a internet');

                const result = await fn();

                if (Array.isArray(result)) {
                    for (const r of result) {
                        if (r && r.error) throw r.error;
                    }
                    return result.map(r => (r && r.data !== undefined) ? r.data : r);
                }

                // Manejo de errores de Supabase
                if (result && result.error) throw result.error;
                return result ? (result.data !== undefined ? result.data : result) : result;
            } catch (err) {
                lastError = err;
                console.warn(`[Tolerancia a fallos] Intento ${i + 1}/${options.retries} fallido:`, err.message);

                if (i < options.retries - 1) {
                    this.state.isRetrying = true;
                    await new Promise(r => setTimeout(r, options.delay * (i + 1)));
                }
            } finally {
                this.state.isRetrying = false;
            }
        }
        if (!options.silent) this.handleGlobalError(lastError);
        throw lastError;
    },

    handleGlobalError(err) {
        console.error('[Error Crítico]', err);
        this.state.error = err.message || 'Error desconocido';

        const appDiv = document.getElementById('app');
        if (appDiv) {
            const isOffline = !navigator.onLine || (this.state.error && (this.state.error.toLowerCase().includes('fetch') || this.state.error.toLowerCase().includes('network')));
            appDiv.innerHTML = this.views.errorState(this.state.error, isOffline);

            // Si el usuario ya había iniciado sesión, mantenemos la navegación inferior para que no se sienta atrapado
            if (this.state.user) {
                appDiv.appendChild(this.components.bottomNav(this.state.currentView || 'dashboard'));
            }
        }

        let notifType = 'Sistema';
        const msg = this.state.error.toLowerCase();
        if (!navigator.onLine || msg.includes('fetch') || msg.includes('network')) notifType = 'Conexión';
        else if (msg.includes('permiso') || msg.includes('rls')) notifType = 'Permisos';
        else if (err instanceof TypeError || err instanceof ReferenceError || msg.includes('defin')) notifType = 'Lógica';

        this.notify(`❌ Error de ${notifType}: ${this.state.error}`, 'error');
    },

    // Sistema de renderizado seguro (Evita listeners duplicados y colisiones)
    mountView(viewName, htmlContent, activeTab = '', shouldScroll = true) {
        const appDiv = document.getElementById('app');
        if (!appDiv) return;

        const currentScroll = window.scrollY; // Guardar posición
        this.state.error = null;
        appDiv.innerHTML = htmlContent;

        if (activeTab && this.state.user) {
            appDiv.appendChild(this.components.bottomNav(activeTab));
        }

        this.state.currentView = viewName;

        // Forzar scroll según necesidad
        if (shouldScroll) {
            window.scrollTo(0, 0);
        } else {
            // Restaurar posición exacta para actualizaciones silenciosas
            requestAnimationFrame(() => window.scrollTo(0, currentScroll));
        }
    },

    async loadDashboard() {
        const user = this.state.user;
        const activeMode = this.state.activeMode || user.role;
        if (!user) return;

        const cacheKey = `dash_${activeMode}`;
        if (this.state.cache.views[cacheKey]) {
            this.mountView('dashboard', this.state.cache.views[cacheKey], 'dashboard');
            this.bindDashboardEvents();
            return; // Usando caché de memoria instantánea
        }

        const meta = await this.getGlobalMeta();
        const isReporter = this.isReporter(user, meta);

        this.mountView('dashboard', this.views.dashboard({ isExec: false, isReporter }), 'dashboard');
        this.bindDashboardEvents();

        try {
            if (activeMode === 'discipulador') {
                const reports = await this.safeCall(() =>
                    window.supabaseClient
                        .from('reports')
                        .select('id, report_date, attendances(attended_friday, attended_saturday)')
                        .eq('reporter_id', user.id)
                );

                let totalExpected = 0;
                let totalAttended = 0;
                (reports || []).forEach(r => {
                    if (!r.attendances) return;
                    r.attendances.forEach(a => {
                        totalExpected++;
                        if (a.attended_friday || a.attended_saturday) totalAttended++;
                    });
                });

                const today = new Date();
                const currentSat = new Date(today);
                currentSat.setDate(today.getDate() + (6 - today.getDay()));
                currentSat.setHours(0, 0, 0, 0);
                const currentWeekStr = currentSat.toISOString().split('T')[0];

                const meta = await this.getGlobalMeta();
                const isReporter = this.isReporter(user, meta);
                const freeWeeks = (meta.freeWeeks || []).map(fw => fw.saturday_date);
                const isFreeWeek = freeWeeks.includes(currentWeekStr);

                const availableWeeks = this.getAvailableWeeks();
                const reportedDates = (reports || []).map(r => r.report_date);

                const pendingWeeks = availableWeeks.filter(w => {
                    const isReported = reportedDates.includes(w.dateStr);
                    const isPast = w.saturday < today;
                    // No marcar como pendientes si es libre o si el usuario no es reportante real
                    return !isReported && isPast && !freeWeeks.includes(w.dateStr) && isReporter;
                });
                const pendingCount = pendingWeeks.length;

                const attendance = totalExpected > 0 ? ((totalAttended / totalExpected) * 100).toFixed(1) + '%' : '0%';
                const reportsCount = reports ? reports.length : 0;

                if (window.location.hash === '#/dashboard' || window.location.hash === '#/') {
                    const html = this.views.dashboard({ isExec: false, attendance, reports: reportsCount, pendingCount, isFreeWeek, isReporter });
                    this.state.cache.views[cacheKey] = html;
                    this.mountView('dashboard', html, 'dashboard');
                    this.bindDashboardEvents();
                }
            } else {
                // Executive Dashboard
                const today = new Date();
                const currentSat = new Date(today);
                currentSat.setDate(today.getDate() + (6 - today.getDay()));
                currentSat.setHours(0, 0, 0, 0);
                const currentWeekStr = currentSat.toISOString().split('T')[0];

                const meta = await this.getGlobalMeta();
                const freeWeeks = (meta.freeWeeks || []).map(fw => fw.saturday_date);
                const isFreeWeek = freeWeeks.includes(currentWeekStr);

                const availableWeeks = this.getAvailableWeeks();

                let discipuladores = [];
                if (activeMode === 'coordinador') {
                    const sectorIds = meta.sectors.filter(s => s.coordinator_id === user.id).map(s => s.id);
                    discipuladores = meta.discipliners.filter(p => sectorIds.includes(p.sector_id) && this.isReporter(p, meta));
                } else {
                    discipuladores = meta.discipliners.filter(p => this.isReporter(p, meta));
                }
                const reporterIds = discipuladores.map(p => p.id);

                let reportsQuery = window.supabaseClient
                    .from('reports')
                    .select('id, report_date, reporter_id, attendances(person_id, attended_friday, attended_saturday, people_assigned(name))')
                    .in('report_date', availableWeeks.map(w => w.dateStr));

                if (activeMode === 'coordinador') {
                    if (reporterIds.length > 0) reportsQuery.in('reporter_id', reporterIds);
                    else reportsQuery.eq('reporter_id', '00000000-0000-0000-0000-000000000000');
                }
                const allRecentReports = await this.safeCall(() => reportsQuery);

                const currentWeekReports = (allRecentReports || []).filter(r => r.report_date === currentWeekStr);
                const totalExpected = discipuladores.length;
                const reportsSent = currentWeekReports.length;
                const missingReports = Math.max(0, totalExpected - reportsSent);

                let attFriday = 0;
                let attSaturday = 0;
                let absentSheepMap = {};

                const weeksDesc = availableWeeks.map(w => w.dateStr).sort((a, b) => b.localeCompare(a));
                let sheepHistory = {};

                (allRecentReports || []).forEach(r => {
                    const weekIdx = weeksDesc.indexOf(r.report_date);
                    if (weekIdx === -1) return;
                    (r.attendances || []).forEach(a => {
                        const personId = a.person_id;
                        if (!meta.sheep.find(s => s.id === personId)) return;
                        if (!a.people_assigned) return;
                        const pName = a.people_assigned.name;
                        if (!sheepHistory[pName]) sheepHistory[pName] = { 0: false, 1: false, 2: false };
                        sheepHistory[pName][weekIdx] = a.attended_friday || a.attended_saturday;

                        if (r.report_date === currentWeekStr) {
                            if (a.attended_friday) attFriday++;
                            if (a.attended_saturday) attSaturday++;
                        }
                    });
                });

                let repeatedAbsences = [];
                for (const [name, hist] of Object.entries(sheepHistory)) {
                    if (hist[0] === false && hist[1] === false) {
                        let count = 2;
                        if (hist[2] === false) count = 3;
                        repeatedAbsences.push({ name, count });
                    }
                }
                repeatedAbsences.sort((a, b) => b.count - a.count);

                const stats = {
                    isExec: true,
                    isFreeWeek,
                    reportsSent,
                    missingReports,
                    totalExpected,
                    currentWeekStr,
                    attFriday,
                    attSaturday,
                    repeatedAbsences: repeatedAbsences.slice(0, 10)
                };

                if (window.location.hash === '#/dashboard' || window.location.hash === '#/') {
                    const html = this.views.dashboard(stats);
                    this.state.cache.views[cacheKey] = html;
                    this.mountView('dashboard', html, 'dashboard');
                    this.bindDashboardEvents();
                }
            }
        } catch (err) {
            console.error('[Dashboard Feed Error]', err);
        }
    },

    async init() {
        this.state.isInitializing = true;

        // 1. Detectores de estado de red (Resiliencia)
        window.addEventListener('online', () => {
            if (this.state.isOnline) return;
            this.state.isOnline = true;
            this.notify('🌐 Conexión restablecida', 'success');
            if (this.state.error) {
                this.state.error = null;
                this.router();
            }
        });

        window.addEventListener('offline', () => {
            this.state.isOnline = false;
            this.notify('📡 Te has quedado sin conexión', 'warning');
        });

        // 2. Sistema de navegación SPA debounced
        window.addEventListener('hashchange', () => {
            if (this._navTimeout) clearTimeout(this._navTimeout);
            this._navTimeout = setTimeout(() => this.router(), 50);
        });

        if (!window.supabaseClient) {
            this.state.isInitializing = false;
            this.handleGlobalError(new Error('El motor de base de datos no está disponible.'));
            return;
        }

        // 3. Gestión de sesión persistente (Estrategia de arranque único)
        try {
            const { data: { session }, error } = await window.supabaseClient.auth.getSession();
            if (error) throw error;

            if (session) {
                // handleSessionData se encargará de setear isInitializing = false y llamar al router
                await this.handleSessionData(session);
            } else {
                this.state.isInitializing = false;
                this.router();
            }
        } catch (err) {
            console.error('Arranque fallido:', err);
            this.state.isInitializing = false;
            this.logout();
        }

        // 4. Suscripción a Auth (Solo eventos posteriores al arranque)
        window.supabaseClient.auth.onAuthStateChange(async (event, session) => {
            if (event === 'SIGNED_IN' && session) {
                // Solo procesamos si no estamos ya inicializando (para evitar doble llamada al arrancar)
                if (this.state.session?.user?.id !== session.user.id) {
                    this.state.error = null;
                    await this.handleSessionData(session);
                }
            } else if (event === 'SIGNED_OUT' || event === 'USER_DELETED') {
                this.state.user = null;
                this.state.session = null;
                this.state.activeMode = null;
                this.state.isInitializing = false;
                this.state.cache = { metadata: null, lastFetch: 0, views: {} };
                sessionStorage.removeItem('elim_active_mode');
                window.location.hash = '#/login';
            }
        });
    },

    async submitReport(isDraft = false) {
        if (this.state.loading) return;

        const dateInput = document.getElementById('reportDate');
        const submitBtn = document.getElementById(isDraft ? 'saveDraftBtn' : 'sendReportBtn');

        if (!dateInput || !submitBtn) return;

        const reportDate = dateInput.value;
        const editReportId = dateInput.getAttribute('data-edit-id');
        const originalReporter = dateInput.getAttribute('data-reporter-id') || this.state.session.user.id;

        const personRows = document.querySelectorAll('.person-row-ref');

        console.log('=== DEBUG GUARDADO ===');
        console.log('1. personRows.length (nodos encontrados en DOM):', personRows.length);

        if (personRows.length === 0) {
            this.notify('❌ No hay discípulos para reportar. Verifica tu lista.', 'error');
            return;
        }

        const attendances = Array.from(personRows).map(row => {
            const friState = row.getAttribute('data-fri') || 'unanswered';
            const satState = row.getAttribute('data-sat') || 'unanswered';
            return {
                person_id: row.getAttribute('data-id'),
                attended_friday: friState === 'present',
                attended_saturday: satState === 'present',
                notes: ''
            };
        });

        console.log('2. array initial attendances generado (salida del DOM):', JSON.stringify(attendances));

        this.state.loading = true;
        const originalText = submitBtn.innerText;
        submitBtn.disabled = true;
        submitBtn.innerHTML = '<span class="spinner-sm"></span> ' + (isDraft ? 'Guardando...' : 'Guardando Reporte...');

        let reportId = editReportId;
        let isNew = !editReportId;

        try {
            const result = await this.safeCall(async () => {
                if (editReportId) {
                    const { error } = await window.supabaseClient.from('reports')
                        .update({ notes: isDraft ? 'DRAFT' : 'FINAL' })
                        .eq('id', editReportId);
                    if (error) throw error;
                    return { id: editReportId, isNew: false };
                } else {
                    const { data, error } = await window.supabaseClient
                        .from('reports')
                        .select('id, notes')
                        .eq('reporter_id', this.state.session.user.id)
                        .eq('report_date', reportDate);

                    if (error) throw error;

                    if (data && data.length > 0) {
                        const activeReport = data[0];
                        if (!isDraft && activeReport.notes === 'FINAL') {
                            throw new Error('Ya existe un reporte finalizado para esta fecha. Edítalo desde Historial.');
                        }
                        const { error: updErr } = await window.supabaseClient.from('reports')
                            .update({ notes: isDraft ? 'DRAFT' : 'FINAL' })
                            .eq('id', activeReport.id);
                        if (updErr) throw updErr;
                        return { id: activeReport.id, isNew: false };
                    }

                    const res = await window.supabaseClient.from('reports').insert({
                        reporter_id: originalReporter,
                        report_date: reportDate,
                        notes: isDraft ? 'DRAFT' : 'FINAL'
                    }).select('id').single();

                    if (res.error) throw res.error;
                    return { id: res.data.id, isNew: true };
                }
            });

            reportId = result.id;
            isNew = result.isNew;

            console.log('3. ID Reporte:', reportId, '| isNew:', isNew);

            // GESTIÓN TRANSACCIONAL CLIENT-SIDE (Estrategia segura anti-vacíos)
            try {
                await this.safeCall(async () => {
                    const { data: existingAtt, error: selErr } = await window.supabaseClient.from('attendances').select('person_id').eq('report_id', reportId);
                    if (selErr) throw selErr;

                    // CORRECCIÓN VITAL: Convertir IDs de DB a String para evitar que fallos de tipado (Integer vs String) vacíen los arrays de diffing.
                    const strExistingIds = (existingAtt || []).map(a => String(a.person_id));

                    const toUpdate = attendances.filter(a => strExistingIds.includes(String(a.person_id)));
                    const toInsert = attendances.filter(a => !strExistingIds.includes(String(a.person_id)));
                    const activeIds = attendances.map(a => String(a.person_id));
                    const toDelete = strExistingIds.filter(pid => !activeIds.includes(pid));

                    console.log('4. a insertar:', toInsert.length, 'a actualizar:', toUpdate.length, 'a borrar:', toDelete.length);
                    console.log('5. toInsert Payload final:', JSON.stringify(toInsert.map(a => ({ ...a, report_id: reportId }))));
                    console.log('=== FIN DEBUG ===');

                    if (toDelete.length > 0) {
                        const { error } = await window.supabaseClient.from('attendances').delete().eq('report_id', reportId).in('person_id', toDelete);
                        if (error) throw error;
                    }

                    if (toUpdate.length > 0) {
                        for (const att of toUpdate) {
                            const { error } = await window.supabaseClient.from('attendances').update({
                                attended_friday: att.attended_friday,
                                attended_saturday: att.attended_saturday
                            }).eq('report_id', reportId).eq('person_id', att.person_id);
                            if (error) throw error;
                        }
                    }

                    if (toInsert.length > 0) {
                        const { error } = await window.supabaseClient.from('attendances').insert(
                            toInsert.map(a => ({ ...a, report_id: reportId }))
                        );
                        if (error) throw error;
                    }
                });
            } catch (relationErr) {
                if (isNew) {
                    console.warn('[Rollback] Falla al guardar asistencias, eliminando reporte huérfano:', reportId);
                    await window.supabaseClient.from('reports').delete().eq('id', reportId);
                }
                throw new Error('Asistencias no guardadas. Intenta nuevamente. ' + relationErr.message);
            }

            this.state.cache.views = {};
            this.state.cache.metadata = null;
            const successMsg = isDraft ? '✅ Borrador guardado' : (isNew ? '🚀 Reporte enviado con éxito' : '📝 Reporte actualizado correctamente');
            this.notify(successMsg, 'success');
            window.location.hash = '#/dashboard';
        } catch (err) {
            console.error('Error al procesar reporte:', err);
            this.notify(`❌ Error: ${err.message}`, 'error');
            submitBtn.disabled = false;
            submitBtn.innerText = originalText;
        } finally {
            this.state.loading = false;
        }
    },

    async loadReportForEdit(reportId) {
        this.mountView('reporte', this.views.loadingState('Cargando reporte para edición...'));

        try {
            const reportData = await this.safeCall(() =>
                window.supabaseClient
                    .from('reports')
                    .select('id, report_date, notes, reporter_id, attendances(person_id, attended_friday, attended_saturday)')
                    .eq('id', reportId)
                    .single()
            );

            if (!reportData) throw new Error('Reporte no encontrado');

            let people = await this.safeCall(() =>
                window.supabaseClient.from('people_assigned').select('*').eq('assigned_to', reportData.reporter_id)
            );

            const attPersonIds = reportData.attendances.map(a => a.person_id);
            const missingPersonIds = attPersonIds.filter(id => !(people || []).find(p => p.id === id));

            if (missingPersonIds.length > 0) {
                const missingPeople = await this.safeCall(() =>
                    window.supabaseClient.from('people_assigned').select('*').in('id', missingPersonIds)
                );
                people = [...(people || []), ...(missingPeople || [])];
            }

            const { friday, saturday } = this.getReportWeekRange(reportData.report_date);
            const weeks = [{ friday, saturday, dateStr: reportData.report_date }];

            const html = this.views.report(people || [], reportData.report_date, weeks);
            this.mountView('reporte', html);

            document.getElementById('reportDate').setAttribute('data-edit-id', reportId);
            document.getElementById('reportDate').setAttribute('data-reporter-id', reportData.reporter_id);

            reportData.attendances.forEach(a => {
                const row = document.querySelector(`.person-row-ref[data-id="${a.person_id}"]`);
                if (row) {
                    row.setAttribute('data-fri', a.attended_friday ? 'present' : 'absent');
                    row.setAttribute('data-sat', a.attended_saturday ? 'present' : 'absent');

                    const btnFriP = row.querySelector('.btn-fri-present');
                    const btnFriA = row.querySelector('.btn-fri-absent');
                    if (a.attended_friday && btnFriP) { btnFriP.classList.add('active'); btnFriP.style.borderColor = '#10B981'; btnFriP.style.background = 'rgba(16, 185, 129, 0.1)'; }
                    if (!a.attended_friday && btnFriA) { btnFriA.classList.add('active'); btnFriA.style.borderColor = '#EF4444'; btnFriA.style.background = 'rgba(239, 68, 68, 0.1)'; }

                    const btnSatP = row.querySelector('.btn-sat-present');
                    const btnSatA = row.querySelector('.btn-sat-absent');
                    if (a.attended_saturday && btnSatP) { btnSatP.classList.add('active'); btnSatP.style.borderColor = '#10B981'; btnSatP.style.background = 'rgba(16, 185, 129, 0.1)'; }
                    if (!a.attended_saturday && btnSatA) { btnSatA.classList.add('active'); btnSatA.style.borderColor = '#EF4444'; btnSatA.style.background = 'rgba(239, 68, 68, 0.1)'; }
                }
            });

            this.bindReportEvents();

            // El selector de fecha debe permanecer habilitado para permitir navegación entre semanas,
            // aunque estemos editando un reporte específico.
            const dateInput = document.getElementById('reportDate');
            if (dateInput) {
                dateInput.style.opacity = '1';
                dateInput.disabled = false;
            }

            const pageTitle = document.querySelector('.header-user h2');
            if (pageTitle) pageTitle.innerText = '✏️ Editar Reporte';

            const sendBtn = document.getElementById('sendReportBtn');
            if (sendBtn) sendBtn.innerText = 'Guardar modificación';

        } catch (err) {
            this.mountView('reporte', this.views.errorState(err.message), 'reporte');
        }
    },

    async handleSessionData(session) {
        this.state.session = session;

        try {
            // 1. Cargar Perfil (Resiliente)
            const profile = await this.safeCall(() =>
                window.supabaseClient
                    .from('profiles')
                    .select('id, name, role, additional_roles, sector_id')
                    .eq('id', session.user.id)
                    .single()
            );

            if (!profile) throw new Error('Usuario no encontrado en la base de datos.');

            const primaryRole = profile.role || 'discipulador';
            const extraRoles = profile.additional_roles || [];
            const allRoles = [...new Set([primaryRole, ...extraRoles])];

            // 2. Cargar Sector (Resiliente)
            let sectorName = 'Comunidad General';
            if (profile.sector_id) {
                const sectorData = await this.safeCall(() =>
                    window.supabaseClient
                        .from('sectors')
                        .select('name')
                        .eq('id', profile.sector_id)
                        .single()
                );
                if (sectorData) sectorName = sectorData.name;
            }

            this.state.user = {
                id: profile.id,
                name: profile.name,
                role: primaryRole,
                allRoles: allRoles,
                sector: sectorName
            };

            // 3. Lógica de redirección inteligente
            const savedMode = sessionStorage.getItem('elim_active_mode');
            const isSavedModeValid = savedMode && allRoles.includes(savedMode);
            const comingFromLogin = window.location.hash === '#/login' || window.location.hash === '';

            if (comingFromLogin) {
                sessionStorage.removeItem('elim_active_mode');
                this.state.activeMode = null;
                if (allRoles.length > 1) {
                    window.location.hash = '#/select-mode';
                } else {
                    this.state.activeMode = primaryRole;
                    sessionStorage.setItem('elim_active_mode', primaryRole);
                    window.location.hash = '#/dashboard';
                }
            } else {
                if (isSavedModeValid) this.state.activeMode = savedMode;
                else if (allRoles.length === 1) {
                    this.state.activeMode = primaryRole;
                    sessionStorage.setItem('elim_active_mode', primaryRole);
                }
            }
        } catch (err) {
            console.error('[Session Error]', err);
            this.handleGlobalError(new Error('No se pudo validar tu perfil: ' + err.message));
        } finally {
            this.state.isInitializing = false;
        }

        // Llamar al router una vez que el estado inicial de la app sea definitivo
        this.router();
    },

    router() {
        if (this._routing) return;
        this._routing = true;

        try {
            const hash = window.location.hash || '#/';

            // 0. Esperar a que la app esté inicializada (Rehidratación de sesión)
            if (this.state.isInitializing) {
                this.mountView('loading', this.views.loadingState('Restableciendo sesión segura...'));
                this._routing = false;
                return;
            }

            // 1. Protección de rutas protegidas
            if (!this.state.user && hash !== '#/login') {
                window.location.hash = '#/login';
                this._routing = false;
                return;
            }

            // 2. Forzar selector de modo si hay varios roles
            if (this.state.user && this.state.user.allRoles.length > 1 && !this.state.activeMode && hash !== '#/select-mode') {
                window.location.hash = '#/select-mode';
                this._routing = false;
                return;
            }

            // 3. Selección de vista
            if (hash.startsWith('#/editar-reporte/')) {
                const reportId = hash.split('/')[2];
                this._routing = false; // IMPORTANTE: liberar semáforo antes de la función asíncrona central
                this.loadReportForEdit(reportId);
                return;
            }

            switch (hash) {
                case '#/login':
                    if (this.state.user) {
                        window.location.hash = '#/dashboard';
                    } else {
                        this.mountView('login', this.views.login());
                        this.bindLoginEvents();
                    }
                    break;

                case '#/select-mode':
                    if (!this.state.user) window.location.hash = '#/login';
                    else {
                        this.mountView('select-mode', this.views.modeSelector());
                        this.bindModeSelectorEvents();
                    }
                    break;

                case '#/dashboard':
                case '#/':
                    this.loadDashboard();
                    break;

                case '#/reporte-sector':
                    this.loadReporteSector();
                    break;

                case '#/reporte':
                    this.renderReportView();
                    break;

                case '#/historial':
                    this.loadHistorial();
                    break;

                case '#/admin':
                    if (this.state.user.role !== 'pastor') window.location.hash = '#/dashboard';
                    else this.loadAdminData();
                    break;

                case '#/estructura':
                    this.loadEstructuraData();
                    break;

                case '#/estadisticas':
                    this.loadEstadisticas();
                    break;

                default:
                    this.mountView('404', `
                        <div style="padding:60px 40px; text-align:center;">
                            <h1 style="font-size:80px; opacity:0.1; margin:0;">404</h1>
                            <p style="color:var(--text-muted); font-size:18px; margin-top:-10px;">Pantalla no encontrada.</p>
                            <br><a href="#/dashboard" class="btn btn-primary" style="max-width:200px; margin:0 auto;">Ir al Inicio</a>
                        </div>
                    `, 'dashboard');
                    break;
            }
        } catch (err) {
            this.handleGlobalError(err);
        } finally {
            this._routing = false;
        }
    },

    // --- LOGICA DE VISTAS DINAMICAS ---
    async renderReportView(dateStr = null) {
        if (!this.state.session) return;

        // Consumir parámetro de salto desde modo edición si existe
        if (!dateStr && this.state._targetDate) {
            dateStr = this.state._targetDate;
            this.state._targetDate = null;
        }

        // 1. Mostrar estado de carga únicamente si NO estamos ya en la vista de reporte.
        // Esto permite que el selector de semana siga visible si el usuario se equivocó 
        // y quiere cambiar a otra semana inmediatamente sin que desaparezca la interfaz.
        const isAlreadyInReport = document.querySelector('.report-view-premium');
        if (!isAlreadyInReport) {
            this.mountView('reporte', this.views.loadingState('Preparando lista de personas...'));
        }

        try {
            // ASEGURAR METADATOS (Semanas Libres)
            await this.getGlobalMeta();
            const weeks = this.getAvailableWeeks();

            // Fetch de reportes (Resiliente)
            const myReports = await this.safeCall(() =>
                window.supabaseClient
                    .from('reports')
                    .select('id, report_date, notes')
                    .eq('reporter_id', this.state.session.user.id)
                    .in('report_date', weeks.map(w => w.dateStr))
            );

            const finalDates = (myReports || []).filter(r => r.notes === 'FINAL').map(r => r.report_date);
            const filteredWeeks = weeks.filter(w => !finalDates.includes(w.dateStr));

            if (filteredWeeks.length === 0) {
                this.notify('✨ Ya has completado todos los reportes de las últimas semanas.', 'success');
                window.location.hash = '#/dashboard';
                return;
            }

            if (!dateStr || finalDates.includes(dateStr)) {
                dateStr = filteredWeeks[0].dateStr;
            }

            const activeDraft = (myReports || []).find(r => r.report_date === dateStr && r.notes === 'DRAFT');
            if (activeDraft) {
                window.location.hash = `#/editar-reporte/${activeDraft.id}`;
                return;
            }

            // Fetch de personas asignadas (Resiliente)
            const people = await this.safeCall(() => this.fetchAssignedPeople());

            if (window.location.hash === '#/reporte') {
                this.mountView('reporte', this.views.report(people || [], dateStr, filteredWeeks));
                this.bindReportEvents();
            }
        } catch (err) {
            console.error('[Report View Error]', err);
            this.mountView('reporte', this.views.errorState(err.message || 'Error al cargar reporte'));
        }
    },

    async fetchAssignedPeople() {
        const { data, error } = await window.supabaseClient
            .from('people_assigned')
            .select('*')
            .eq('assigned_to', this.state.session.user.id);
        if (error) throw error;
        return data || [];
    },

    async loadHistorial(filters = {}) {
        if (!this.state.user || !this.state.session) return;
        const isPastor = this.state.user.role === 'pastor';

        // Mantener el límite actual o incrementarlo si es 'loadMore', sino resetear
        if (filters.loadMore) {
            this.state.historyLimit = (this.state.historyLimit || 50) + 50;
        } else {
            this.state.historyLimit = 50; // Reset por defecto o nueva búsqueda
        }

        const cacheKey = `historial_${JSON.stringify(filters)}`;
        if (!filters.loadMore && this.state.cache.views[cacheKey] && !this._forceRefresh) {
            this.mountView('historial', this.state.cache.views[cacheKey], 'historial');
            this.bindHistorialEvents(filters);
            return;
        }

        // Mostrar spinner flotante si es Cargar Más, si no, estado total
        if (!filters.loadMore) {
            this.mountView('historial', this.views.loadingState('Preparando historial...'), 'historial');
        } else {
            const btn = document.getElementById('btnLoadMoreHistory');
            if (btn) btn.innerHTML = '<span class="spinner-sm" style="width:16px; height:16px; border-width:2px; margin-right:8px;"></span> Cargando...';
        }

        try {
            // Fetch de metadatos (caché)
            let filterMetadata = null;
            if (isPastor) {
                const meta = await this.getGlobalMeta();
                filterMetadata = {
                    sectors: meta.sectors,
                    discipliners: meta.discipliners,
                    sheep: meta.sheep
                };

                if (filters.sector_id) {
                    filterMetadata.discipliners = filterMetadata.discipliners.filter(d => d.sector_id === filters.sector_id);
                }
                if (filters.reporter_id) {
                    filterMetadata.sheep = filterMetadata.sheep.filter(s => s.assigned_to === filters.reporter_id);
                }
            }

            // Construir Query
            let query = window.supabaseClient.from('reports').select('id, report_date, notes, reporter_id, reporter:profiles!reports_reporter_id_fkey(name, sector_id), attendances(person_id, attended_friday, attended_saturday, notes, people_assigned(name))').order('report_date', { ascending: false }).order('id', { ascending: false });

            if (!isPastor) {
                query = query.eq('reporter_id', this.state.session.user.id);
            } else {
                if (filters.reporter_id) query = query.eq('reporter_id', filters.reporter_id);
                if (filters.sector_id) {
                    const profilesInSector = await this.safeCall(() =>
                        window.supabaseClient.from('profiles').select('id').eq('sector_id', filters.sector_id)
                    );
                    query = query.in('reporter_id', (profilesInSector || []).map(p => p.id));
                }
                if (filters.person_id) {
                    const atts = await this.safeCall(() =>
                        window.supabaseClient.from('attendances').select('report_id').eq('person_id', filters.person_id)
                    );
                    query = query.in('id', [...new Set((atts || []).map(a => a.report_id))]);
                }
            }

            const reports = await this.safeCall(() => query.limit(this.state.historyLimit));
            const hasMore = reports && reports.length === this.state.historyLimit;

            // Renderizado final
            if (window.location.hash === '#/historial') {
                const html = this.views.historial(reports || [], filterMetadata, filters, hasMore);
                if (!filters.loadMore) this.state.cache.views[cacheKey] = html; // Cacheamos solo si es primer load
                this.mountView('historial', html, 'historial');
                this.bindHistorialEvents(filters);
            }
        } catch (err) {
            console.error('[Historial Load Error]', err);
            this.mountView('historial', this.views.errorState(err.message || 'Error cargando historial'), 'historial');
        }
    },

    bindHistorialEvents(currentFilters = {}) {
        const fSector = document.getElementById('fSector');
        const fLeader = document.getElementById('fLeader');
        const fSheep = document.getElementById('fSheep');

        const getFilters = () => ({
            sector_id: fSector ? fSector.value : '',
            reporter_id: fLeader ? fLeader.value : '',
            person_id: fSheep ? fSheep.value : ''
        });

        [fSector, fLeader, fSheep].forEach(el => {
            if (el) el.onchange = () => this.loadHistorial(getFilters());
        });

        const btnClear = document.getElementById('btnClearFilters');
        if (btnClear) {
            btnClear.onclick = () => this.loadHistorial({});
        }

        const btnLoadMore = document.getElementById('btnLoadMoreHistory');
        if (btnLoadMore) {
            btnLoadMore.onclick = () => {
                this.loadHistorial({ ...currentFilters, loadMore: true });
            };
        }

        // Doble Clic para Detalle Completo de Reporte (Para todos los roles que puedan leer el reporte. Protegido por RLS)
        if (!this._historialDblClickBound) {
            this._historialDblClickBound = true;
            const appDiv = document.getElementById('app');
            appDiv.addEventListener('dblclick', (e) => {
                if (window.location.hash !== '#/historial') return;
                const card = e.target.closest('.history-card');
                if (card) {
                    const id = card.getAttribute('data-report-id');
                    if (id) {
                        window.getSelection().removeAllRanges();
                        this.showReportDetailModal(id);
                    }
                }
            });
        }

        // Evento directo de descarga a PDF desde Historial
        document.querySelectorAll('.btn-download-pdf-direct').forEach(btn => {
            btn.onclick = (e) => {
                e.stopPropagation();
                const id = btn.getAttribute('data-id');
                this.downloadReportPdf(id, btn);
            };
        });

        // Eventos de edición
        document.querySelectorAll('.btn-edit-report').forEach(btn => {
            btn.onclick = (e) => {
                e.stopPropagation();
                window.location.hash = `#/editar-reporte/${btn.getAttribute('data-id')}`;
            };
        });

        // Eventos de eliminación
        document.querySelectorAll('.btn-delete-report').forEach(btn => {
            btn.onclick = async (e) => {
                e.stopPropagation();
                const id = btn.getAttribute('data-id');
                if (await this.confirmDialog('¿Eliminar Reporte?', 'Esta acción borrará permanentemente este registro de asistencia. No se puede deshacer.', 'Sí, eliminar permanentemente')) {
                    try {
                        this.notify('⏳ Eliminando reporte...', 'info');

                        // 1. Eliminar hijos primero (por si no hay ON DELETE CASCADE en Supabase)
                        const { error: attError } = await window.supabaseClient
                            .from('attendances')
                            .delete()
                            .eq('report_id', id);

                        if (attError) throw attError;

                        // 2. Eliminar el reporte principal
                        const { error: repError } = await window.supabaseClient
                            .from('reports')
                            .delete()
                            .eq('id', id);

                        if (repError) throw repError;

                        this.notify('✅ Reporte eliminado correctamente');
                        this.loadHistorial(getFilters());
                    } catch (err) {
                        console.error('Error al eliminar:', err);
                        this.notify(`❌ No se pudo eliminar: ${err.message || 'Verifica los permisos RLS'}`, 'error');
                    }
                }
            };
        });
    },
    async downloadReportPdf(reportId, btnElement = null) {
        let originalText = '';
        let originalOpacity = '1';
        let originalPointer = 'auto';

        if (btnElement) {
            if (btnElement.disabled) return;
            const textSpan = btnElement.tagName === 'BUTTON' && btnElement.querySelector('.btn-text') ? btnElement.querySelector('.btn-text') : null;
            originalText = textSpan ? textSpan.innerText : btnElement.innerText;
            originalOpacity = btnElement.style.opacity;
            originalPointer = btnElement.style.pointerEvents;

            btnElement.disabled = true;
            btnElement.style.opacity = '0.6';
            btnElement.style.pointerEvents = 'none';
            if (textSpan) {
                textSpan.innerText = '⏳';
            } else {
                btnElement.innerText = '⏳';
            }
        }

        try {
            const isHtml2PdfLoaded = typeof window.html2pdf !== 'undefined';
            if (!isHtml2PdfLoaded) {
                throw new Error('No se pudo cargar la librería PDF. Revisa tu conexión.');
            }

            const reportData = await this.safeCall(() =>
                window.supabaseClient
                    .from('reports')
                    .select(`
                        id, report_date, notes, reporter_id,
                        reporter:profiles!reports_reporter_id_fkey(name, sector_id),
                        attendances (
                            person_id, attended_friday, attended_saturday, notes,
                            people_assigned (name)
                        )
                    `)
                    .eq('id', reportId)
                    .single()
            );

            if (!reportData) throw new Error('No se encontró el reporte.');

            let sectorName = 'Comunidad General';
            if (reportData.reporter?.sector_id) {
                const s = await this.safeCall(() => window.supabaseClient.from('sectors').select('name').eq('id', reportData.reporter.sector_id).single());
                if (s) sectorName = s.name;
            }

            const isDraft = reportData.notes === 'DRAFT';
            const stateLabel = isDraft ? 'BORRADOR' : 'ENVIADO';
            const stateColor = isDraft ? '#F59E0B' : '#10B981';
            const fDate = this.formatFriendlyDate(new Date(reportData.report_date + 'T12:00:00'));

            // Construir Estructura Oculta
            const pdfHtml = `
                <div style="padding: 40px; font-family: 'Inter', sans-serif; color: #0F172A; background: white;">
                    <div style="text-align: center; margin-bottom: 30px; border-bottom: 2px solid #E2E8F0; padding-bottom: 20px;">
                        <h1 style="margin: 0; color: #1E293B; font-size: 24px;">Reporte de Asistencia</h1>
                        <p style="margin: 5px 0 0; color: #64748B; font-size: 14px;">Fecha: ${fDate}</p>
                    </div>
                    
                    <div style="display: flex; justify-content: space-between; margin-bottom: 30px; background: #F8FAFC; padding: 20px; border-radius: 12px; border: 1px solid #E2E8F0;">
                        <div>
                            <strong style="color: #64748B; font-size: 11px; text-transform: uppercase;">Discipulador Responsable</strong>
                            <div style="font-size: 16px; font-weight: bold; color: #1E293B; margin-top: 4px;">${reportData.reporter?.name || 'Desconocido'}</div>
                            <div style="font-size: 13px; color: #475569; margin-top: 2px;">${sectorName}</div>
                        </div>
                        <div style="text-align: right;">
                            <strong style="color: #64748B; font-size: 11px; text-transform: uppercase;">Estado del Reporte</strong>
                            <div style="font-size: 14px; font-weight: bold; color: ${stateColor}; margin-top: 4px;">${stateLabel}</div>
                        </div>
                    </div>

                    <h3 style="margin: 0 0 15px; color: #1E293B; font-size: 16px; border-bottom: 1px solid #E2E8F0; padding-bottom: 10px;">Detalle de Miembros</h3>
                    
                    <table style="width: 100%; border-collapse: collapse; margin-bottom: 30px;">
                        <thead>
                            <tr style="background: #F1F5F9;">
                                <th style="padding: 12px; text-align: left; font-size: 13px; color: #475569; border-bottom: 2px solid #E2E8F0;">Nombre de la Persona</th>
                                <th style="padding: 12px; text-align: center; font-size: 13px; color: #475569; border-bottom: 2px solid #E2E8F0; width: 80px;">Viernes</th>
                                <th style="padding: 12px; text-align: center; font-size: 13px; color: #475569; border-bottom: 2px solid #E2E8F0; width: 80px;">Sábado</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${(reportData.attendances || []).map(a => `
                                <tr>
                                    <td style="padding: 12px; border-bottom: 1px solid #E2E8F0; font-size: 14px; font-weight: 500;">${a.people_assigned?.name || 'Vínculo roto'}</td>
                                    <td style="padding: 12px; border-bottom: 1px solid #E2E8F0; text-align: center;">${a.attended_friday ? '<span style="color:#10B981; font-weight:bold;">Sí</span>' : '<span style="color:#EF4444; font-weight:bold;">No</span>'}</td>
                                    <td style="padding: 12px; border-bottom: 1px solid #E2E8F0; text-align: center;">${a.attended_saturday ? '<span style="color:#10B981; font-weight:bold;">Sí</span>' : '<span style="color:#EF4444; font-weight:bold;">No</span>'}</td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>

                    <div style="font-size: 11px; color: #94A3B8; text-align: center; margin-top: 50px; border-top: 1px solid #E2E8F0; padding-top: 20px;">
                        Reporte generado por <strong>Elim Reporting App</strong> el ${new Date().toLocaleDateString('es-ES', { year: 'numeric', month: 'long', day: 'numeric', hour: '2-digit', minute: '2-digit' })}.<br>
                        Confidencial - Solo para uso interno.
                    </div>
                </div>
            `;

            const tempDiv = document.createElement('div');
            tempDiv.innerHTML = pdfHtml;

            const reportIdStr = (reportData.reporter?.name || 'Desconocido').replace(/\s+/g, '_');
            const dateStr = reportData.report_date.replace(/-/g, '');

            const opt = {
                margin: [10, 10, 10, 10],
                filename: `Reporte_${reportIdStr}_${dateStr}.pdf`,
                image: { type: 'jpeg', quality: 0.98 },
                html2canvas: { scale: 2, useCORS: true, letterRendering: true },
                jsPDF: { unit: 'mm', format: 'a4', orientation: 'portrait' }
            };

            await window.html2pdf().set(opt).from(tempDiv).save();
            this.notify('✅ Reporte PDF descargado', 'success');

        } catch (err) {
            console.error('Error exportando PDF:', err);
            this.notify(`❌ Falla en PDF: ${err.message}`, 'error');
        } finally {
            if (btnElement) {
                btnElement.disabled = false;
                btnElement.style.opacity = originalOpacity;
                btnElement.style.pointerEvents = originalPointer;
                const textSpan = btnElement.tagName === 'BUTTON' && btnElement.querySelector('.btn-text') ? btnElement.querySelector('.btn-text') : null;
                if (textSpan) {
                    textSpan.innerText = originalText;
                } else {
                    btnElement.innerText = originalText;
                }
            }
        }
    },

    async showReportDetailModal(reportId) {
        let overlay = document.getElementById('reportDetailModal');
        if (!overlay) {
            overlay = document.createElement('div');
            overlay.className = 'modal-overlay';
            overlay.id = 'reportDetailModal';
            document.body.appendChild(overlay);
        }

        overlay.innerHTML = `
            <div class="modal-content" style="max-width: 650px; width: 90%; text-align:center; padding: 40px;">
                <div class="spinner-sm" style="border-color: var(--primary); border-top-color: transparent;"></div>
                <p style="margin-top: 16px; color: var(--text-muted); font-weight: 600;">Cargando detalle del reporte...</p>
            </div>
        `;

        try {
            const reportData = await this.safeCall(() =>
                window.supabaseClient
                    .from('reports')
                    .select(`
                        id, report_date, notes, reporter_id,
                        reporter:profiles!reports_reporter_id_fkey(name, sector_id),
                        attendances (
                            person_id, attended_friday, attended_saturday, notes,
                            people_assigned (name)
                        )
                    `)
                    .eq('id', reportId)
                    .single()
            );

            if (!reportData) throw new Error('No se encontró el reporte en la base de datos.');

            // Si hay un ID de sector, intentamos traer su nombre para que el reporte sea completo
            let sectorName = 'Comunidad General';
            if (reportData.reporter?.sector_id) {
                const s = await this.safeCall(() => window.supabaseClient.from('sectors').select('name').eq('id', reportData.reporter.sector_id).single());
                if (s) sectorName = s.name;
            }

            const isDraft = reportData.notes === 'DRAFT';
            const stateLabel = isDraft ? 'BORRADOR' : 'ENVIADO';
            const stateColor = isDraft ? '#F59E0B' : '#10B981';
            const fDate = this.formatFriendlyDate(new Date(reportData.report_date + 'T12:00:00'));

            let attHtml = (reportData.attendances || []).map(a => `
                <div style="display:flex; justify-content:space-between; padding:12px 16px; border-bottom:1px solid rgba(0,0,0,0.03); align-items:center;">
                    <span style="font-weight:700; color:var(--text-main); font-size:14px;">${a.people_assigned?.name || 'Vínculo roto'}</span>
                    <div style="display:flex; gap:10px;">
                        <span class="pill ${a.attended_friday ? 'pill-success' : 'pill-danger'}" style="width:30px; height:30px; display:inline-block; line-height:30px; text-align:center; border-radius:50%; font-weight:900; font-size:12px; padding:0 !important; box-shadow: 0 4px 8px rgba(0,0,0,0.1);">V</span>
                        <span class="pill ${a.attended_saturday ? 'pill-success' : 'pill-danger'}" style="width:30px; height:30px; display:inline-block; line-height:30px; text-align:center; border-radius:50%; font-weight:900; font-size:12px; padding:0 !important; box-shadow: 0 4px 8px rgba(0,0,0,0.1);">S</span>
                    </div>
                </div>
            `).join('');

            if (!attHtml) attHtml = '<p style="text-align:center; padding:24px; color:var(--text-muted); margin:0;">No hay asistencia registrada</p>';

            // Estructura oculta para el PDF (diseño corporativo y formal)
            const pdfHtml = `
                <div style="padding: 40px; font-family: 'Inter', sans-serif; color: #0F172A; background: white;">
                    <div style="text-align: center; margin-bottom: 30px; border-bottom: 2px solid #E2E8F0; padding-bottom: 20px;">
                        <h1 style="margin: 0; color: #1E293B; font-size: 24px;">Reporte de Asistencia</h1>
                        <p style="margin: 5px 0 0; color: #64748B; font-size: 14px;">Fecha: ${fDate}</p>
                    </div>
                    
                    <div style="display: flex; justify-content: space-between; margin-bottom: 30px; background: #F8FAFC; padding: 20px; border-radius: 12px; border: 1px solid #E2E8F0;">
                        <div>
                            <strong style="color: #64748B; font-size: 11px; text-transform: uppercase;">Discipulador Responsable</strong>
                            <div style="font-size: 16px; font-weight: bold; color: #1E293B; margin-top: 4px;">${reportData.reporter?.name || 'Desconocido'}</div>
                            <div style="font-size: 13px; color: #475569; margin-top: 2px;">${sectorName}</div>
                        </div>
                        <div style="text-align: right;">
                            <strong style="color: #64748B; font-size: 11px; text-transform: uppercase;">Estado del Reporte</strong>
                            <div style="font-size: 14px; font-weight: bold; color: ${stateColor}; margin-top: 4px;">${stateLabel}</div>
                        </div>
                    </div>

                    <h3 style="margin: 0 0 15px; color: #1E293B; font-size: 16px; border-bottom: 1px solid #E2E8F0; padding-bottom: 10px;">Detalle de Miembros</h3>
                    
                    <table style="width: 100%; border-collapse: collapse; margin-bottom: 30px;">
                        <thead>
                            <tr style="background: #F1F5F9;">
                                <th style="padding: 12px; text-align: left; font-size: 13px; color: #475569; border-bottom: 2px solid #E2E8F0;">Nombre de la Persona</th>
                                <th style="padding: 12px; text-align: center; font-size: 13px; color: #475569; border-bottom: 2px solid #E2E8F0; width: 80px;">Viernes</th>
                                <th style="padding: 12px; text-align: center; font-size: 13px; color: #475569; border-bottom: 2px solid #E2E8F0; width: 80px;">Sábado</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${(reportData.attendances || []).map(a => `
                                <tr>
                                    <td style="padding: 12px; border-bottom: 1px solid #E2E8F0; font-size: 14px; font-weight: 500;">${a.people_assigned?.name || 'Vínculo roto'}</td>
                                    <td style="padding: 12px; border-bottom: 1px solid #E2E8F0; text-align: center;">${a.attended_friday ? '<span style="color:#10B981; font-weight:bold;">Sí</span>' : '<span style="color:#EF4444; font-weight:bold;">No</span>'}</td>
                                    <td style="padding: 12px; border-bottom: 1px solid #E2E8F0; text-align: center;">${a.attended_saturday ? '<span style="color:#10B981; font-weight:bold;">Sí</span>' : '<span style="color:#EF4444; font-weight:bold;">No</span>'}</td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>

                    <div style="font-size: 11px; color: #94A3B8; text-align: center; margin-top: 50px; border-top: 1px solid #E2E8F0; padding-top: 20px;">
                        Reporte generado por <strong>Elim Reporting App</strong> el ${new Date().toLocaleDateString('es-ES', { year: 'numeric', month: 'long', day: 'numeric', hour: '2-digit', minute: '2-digit' })}.<br>
                        Confidencial - Solo para uso interno.
                    </div>
                </div>
            `;

            overlay.innerHTML = `
                <div class="modal-content" style="max-width: 600px; width: 90%; max-height: 90vh; display: flex; flex-direction: column; padding: 0; border-radius: 24px; overflow: hidden;">
                    <div style="padding: 24px; background: #F8FAFC; border-bottom: 1px solid rgba(0,0,0,0.05); display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <h3 style="margin:0; font-size: 20px; font-weight: 800; color: var(--primary);">Detalle de Reporte</h3>
                            <p style="margin:4px 0 0 0; font-size:13px; color:var(--text-muted); font-weight: 600;">${fDate}</p>
                        </div>
                        <button id="closeDetailIcon" style="background:none; border:none; font-size:28px; line-height:1; padding:0; color:var(--text-muted); cursor:pointer;">&times;</button>
                    </div>

                    <div style="padding: 24px; overflow-y: auto; flex: 1;">
                        <div style="display:flex; justify-content:space-between; margin-bottom: 24px; background: white; border: 1px solid rgba(0,0,0,0.05); box-shadow: 0 4px 12px rgba(0,0,0,0.02); padding: 16px; border-radius: 16px;">
                            <div>
                                <div style="font-size:10px; font-weight:800; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.05em;">Discipulador</div>
                                <div style="font-weight:800; color:var(--primary); font-size:15px; margin-top:2px;">${reportData.reporter?.name || 'Desconocido'}</div>
                                <div style="font-size:12px; color:var(--text-muted); margin-top:2px; font-weight: 600;">${sectorName}</div>
                            </div>
                            <div style="text-align:right;">
                                <div style="font-size:10px; font-weight:800; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.05em;">Estado</div>
                                <div style="font-weight:900; color:${stateColor}; font-size:14px; margin-top:4px;">${stateLabel}</div>
                            </div>
                        </div>
                        
                        <h4 style="margin:0 0 12px 0; font-size:12px; color:var(--text-muted); font-weight:800; text-transform:uppercase; letter-spacing:0.1em;">Registro de Asistencia Detallado</h4>
                        <div style="border: 1px solid rgba(0,0,0,0.05); border-radius: 16px; overflow:hidden; background: white;">
                            ${attHtml}
                        </div>
                    </div>

                    <div style="padding: 20px 24px; background: #F8FAFC; border-top: 1px solid rgba(0,0,0,0.05); display:flex; justify-content:end; gap:12px; flex-wrap: wrap;">
                        <button id="downloadPdfBtn" class="btn" style="padding: 12px 24px; background:white; color:var(--primary); border:1px solid rgba(37,99,235,0.2); box-shadow:0 4px 12px rgba(0,0,0,0.02); display:flex; align-items:center; justify-content:center; gap:8px;">
                            <span>📄</span> <span class="btn-text">Descargar PDF</span>
                        </button>
                        <button class="btn btn-primary close-modal-btn" style="padding: 12px 24px;">Cerrar Vista</button>
                    </div>
                </div>
            `;

            // Lógica de cierre del modal
            const closeAction = () => overlay.remove();
            document.getElementById('closeDetailIcon').onclick = closeAction;
            document.querySelector('.close-modal-btn').onclick = closeAction;

            // Lógica para exportación a PDF (ahora reutiliza el método universal asegurando permisos y UX)
            const btnDownload = document.getElementById('downloadPdfBtn');
            if (btnDownload) {
                btnDownload.onclick = () => this.downloadReportPdf(reportId, btnDownload);
            }

        } catch (err) {
            console.error('Error detallando modal:', err);
            overlay.innerHTML = `
                <div class="modal-content" style="max-width: 400px; text-align:center; padding: 40px;">
                    <div style="font-size: 40px; margin-bottom: 16px;">⚠️</div>
                    <h3 style="color:var(--danger); margin:0 0 12px 0;">Ocurrió un error</h3>
                    <p style="color:var(--text-muted); margin:0 0 24px 0;">${err.message || 'No se pudo cargar la información del reporte'}</p>
                    <button class="btn btn-primary" onclick="document.getElementById('reportDetailModal').remove()">Entendido</button>
                </div>
            `;
        }
    },

    async loadAdminData(silent = false) {
        if (!silent) {
            this.mountView('admin', this.views.loadingState('Abriendo panel de administración...'), 'admin');
        }

        try {
            const meta = await this.getGlobalMeta();
            const sectorsRes = meta.sectors;
            const profilesRes = meta.profiles;
            const peopleRes = meta.sheep;

            const { sectorMap, unassignedSheep } = this.getSectorMap(sectorsRes, profilesRes, peopleRes);

            if (window.location.hash === '#/admin') {
                this.mountView('admin', this.views.admin(sectorsRes, profilesRes, peopleRes, sectorMap, unassignedSheep), 'admin', !silent);
                this.bindAdminEvents(profilesRes);

                // Renderizar grid de semanas libres
                const grid = document.getElementById('freeWeeksGrid');
                if (grid) {
                    const currentYear = new Date().getFullYear();
                    const allSats = this.getYearSaturdays(currentYear);
                    const configured = (meta.freeWeeks || []).map(f => f.saturday_date);

                    grid.innerHTML = allSats.map(sat => {
                        const dateStr = sat.toISOString().split('T')[0];
                        const isFree = configured.includes(dateStr);
                        const label = sat.toLocaleDateString('es-ES', { day: 'numeric', month: 'short' });

                        return `
                            <label style="display:flex; align-items:center; gap:10px; background:white; padding:10px 14px; border-radius:12px; border:1px solid ${isFree ? 'var(--accent)' : 'rgba(0,0,0,0.05)'}; cursor:pointer; font-size:13px; font-weight:600; box-shadow: ${isFree ? '0 4px 12px var(--accent-glow)' : 'none'}; transition:all 0.2s;">
                                <input type="checkbox" class="free-week-checkbox" value="${dateStr}" ${isFree ? 'checked' : ''} style="width:18px; height:18px; accent-color:var(--accent);">
                                <span>${label}</span>
                            </label>
                        `;
                    }).join('');
                }
            }
        } catch (err) {
            console.error('Error cargando Admin:', err);
            this.mountView('admin', this.views.errorState(err.message || 'Error al abrir panel de administración'), 'admin');
        }
    },

    async loadEstructuraData() {
        this.mountView('estructura', this.views.loadingState('Construyendo estructura organizacional...'), 'estructura');

        try {
            const meta = await this.getGlobalMeta();

            if (window.location.hash === '#/estructura') {
                const { sectorMap, unassignedSheep } = this.getSectorMap(meta.sectors, meta.profiles, meta.sheep);
                this.mountView('estructura', this.views.estructura(meta.sectors, meta.profiles, meta.sheep, sectorMap, unassignedSheep), 'estructura');
                this.bindEstructuraEvents();
            }
        } catch (err) {
            console.error('Estructura error:', err);
            this.mountView('estructura', this.views.errorState(err.message || 'Error cargando estructura'), 'estructura');
        }
    },

    // fetchAssignedPeople duplicado eliminado

    async saveSector(name) {
        const { error } = await window.supabaseClient.from('sectors').insert([{ name }]);
        return !error;
    },

    async savePerson(name, assigned_to) {
        // Normalizar assigned_to: si es vacío o falsy, usar null para evitar errores de UUID en Postgres
        const leaderId = assigned_to || null;
        const { error } = await window.supabaseClient.from('people_assigned').insert([{ name, assigned_to: leaderId }]);
        return !error;
    },

    async deletePerson(personId) {
        const { error } = await window.supabaseClient.from('people_assigned').delete().eq('id', personId);
        return !error;
    },

    async saveUserSector(userId, sectorId) {
        const sid = sectorId || null;
        const { error } = await window.supabaseClient.from('profiles').update({ sector_id: sid }).eq('id', userId);
        return !error;
    },

    async saveReport(reportHeader, attendancesList) {
        try {
            const { data: reportResult, error: reportError } = await window.supabaseClient.from('reports').insert([reportHeader]).select('id').single();
            if (reportError) throw reportError;
            const attendancesToInsert = attendancesList.map(a => ({ ...a, report_id: reportResult.id }));
            if (attendancesToInsert.length > 0) {
                const { error: attError } = await window.supabaseClient.from('attendances').insert(attendancesToInsert);
                if (attError) throw attError;
            }
            return true;
        } catch (error) {
            console.error('Error guardando reporte:', error);
            return false;
        }
    },

    async login(email, password) {
        this.state.loading = true;
        const btn = document.getElementById('loginBtn');
        if (btn) btn.innerText = 'Autenticando...';
        const { error } = await window.supabaseClient.auth.signInWithPassword({ email, password });
        if (error) {
            this.notify('Error: ' + error.message, 'error');
            if (btn) btn.innerText = 'Iniciar Sesión';
        }
        this.state.loading = false;
    },

    async logout() {
        await window.supabaseClient.auth.signOut();
    },

    async unassignPerson(personId) {
        // Intentar en people_assigned que es la tabla confirmada para personas
        const { error, data } = await window.supabaseClient
            .from('people_assigned')
            .update({ assigned_to: null })
            .eq('id', personId)
            .select();

        if (data && data.length > 0) return true;

        if (error) {
            console.error('Error de Supabase:', error);
            throw error;
        }

        throw new Error('El registro no se actualizó (0 filas afectadas). Esto suele suceder por Políticas de Seguridad (RLS) en Supabase que impiden a un Líder o Coordinador modificar personas que no le pertenecen directamente.');
    },

    async reassignPerson(personId, newDisciplerId) {
        const leaderId = newDisciplerId || null;

        const { error, data } = await window.supabaseClient
            .from('people_assigned')
            .update({ assigned_to: leaderId })
            .eq('id', personId)
            .select();

        if (data && data.length > 0) return true;

        if (error) {
            console.error('Error de Supabase:', error);
            throw error;
        }

        throw new Error('No se pudo reasignar. Verifique que tenga permisos (RLS) para editar este registro.');
    },

    // --- NOTIFICACIONES Y MODALES ---
    notify(message, type = 'success') {
        let container = document.getElementById('toast-container');
        if (!container) {
            container = document.createElement('div');
            container.id = 'toast-container';
            document.body.appendChild(container);
        }

        const icons = { success: '✅', error: '❌', info: 'ℹ️' };
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.innerHTML = `<span>${icons[type] || ''}</span> <span>${message}</span>`;

        container.appendChild(toast);

        setTimeout(() => {
            toast.classList.add('out');
            setTimeout(() => toast.remove(), 400);
        }, 3000);
    },

    confirmDialog(title, text, confirmText = 'Confirmar') {
        return new Promise((resolve) => {
            const overlay = document.createElement('div');
            overlay.className = 'modal-overlay';
            overlay.innerHTML = `
                <div class="modal-content">
                    <div class="modal-icon">❓</div>
                    <div class="modal-title">${title}</div>
                    <div class="modal-text">${text}</div>
                    <div class="modal-actions">
                        <button class="btn-modal btn-modal-cancel" id="modalCancel">Cancelar</button>
                        <button class="btn-modal btn-modal-confirm" id="modalConfirm">${confirmText}</button>
                    </div>
                </div>
            `;
            document.body.appendChild(overlay);

            document.getElementById('modalCancel').onclick = () => {
                overlay.remove();
                resolve(false);
            };
            document.getElementById('modalConfirm').onclick = () => {
                overlay.remove();
                resolve(true);
            };
        });
    },

    promptDialog(title, text, options = []) {
        return new Promise((resolve) => {
            const overlay = document.createElement('div');
            overlay.className = 'modal-overlay';

            let inputHtml = options.length > 0
                ? `<select id="modalInput" class="form-control" style="margin-bottom:20px;">
                    <option value="">-- Seleccionar Discipulador --</option>
                    ${options.map(o => `<option value="${o.value}">${o.label}</option>`).join('')}
                   </select>`
                : `<input type="text" id="modalInput" class="form-control" style="margin-bottom:20px;" placeholder="Escribe aquí...">`;

            overlay.innerHTML = `
                <div class="modal-content">
                    <div class="modal-icon">👤</div>
                    <div class="modal-title">${title}</div>
                    <div class="modal-text">${text}</div>
                    ${inputHtml}
                    <div class="modal-actions">
                        <button class="btn-modal btn-modal-cancel" id="modalCancel">Cancelar</button>
                        <button class="btn-modal btn-modal-confirm" id="modalConfirm">Confirmar</button>
                    </div>
                </div>
            `;
            document.body.appendChild(overlay);

            document.getElementById('modalCancel').onclick = () => {
                overlay.remove();
                resolve(null);
            };
            document.getElementById('modalConfirm').onclick = () => {
                const val = document.getElementById('modalInput').value;
                overlay.remove();
                resolve(val || null);
            };
        });
    },

    // --- UTILIDADES DE EXPORTACIÓN ---
    async downloadPDF() {
        const element = document.querySelector('.dashboard-content');
        if (!element) return;

        // Opciones de configuración para el PDF
        const opt = {
            margin: [10, 10, 10, 10],
            filename: `Reporte_Elim_${document.getElementById('reportDate')?.value || 'Semanal'}.pdf`,
            image: { type: 'jpeg', quality: 0.98 },
            html2canvas: { scale: 2, useCORS: true, letterRendering: true },
            jsPDF: { unit: 'mm', format: 'a4', orientation: 'portrait' }
        };

        this.notify('Generando PDF...', 'info');

        try {
            // Clonar el elemento para no afectar la vista actual si hacemos cambios temporales
            const clone = element.cloneNode(true);

            // Podríamos añadir un encabezado extra al clon aquí
            const header = document.createElement('div');
            header.innerHTML = `<h1 style="color:#0F172A; text-align:center; font-family:Inter, sans-serif;">Reporte General de Discipulado</h1><hr style="border:1px solid #f1f5f9; margin-bottom:20px;">`;
            clone.prepend(header);

            // Generar
            await html2pdf().set(opt).from(clone).save();
            this.notify('✅ PDF Descargado');
        } catch (err) {
            console.error('Error generando PDF:', err);
            this.notify('Error al generar PDF', 'error');
        }
    },

    // --- UTILIDADES DE FECHAS Y MÓDULOS ---
    getYearSaturdays(year) {
        const saturdays = [];
        let d = new Date(year, 0, 1);
        // Ir al primer sábado
        while (d.getDay() !== 6) {
            d.setDate(d.getDate() + 1);
        }
        while (d.getFullYear() === year) {
            saturdays.push(new Date(d));
            d.setDate(d.getDate() + 7);
        }
        return saturdays;
    },

    isReporter(p, meta) {
        if (!p || !meta) return false;

        // CONDICIÓN 1: Tiene personas asignadas directamente (Reportante operativo)
        const hasSheep = (meta.sheep || []).some(s => s.assigned_to === p.id);

        // CONDICIÓN 2: Su rol primario en el sistema es 'discipulador'
        const isDiscipuladorRole = p.role === 'discipulador';

        // Un reportero es alguien que tiene ovejas a su cargo O es un discipulador nominal en la estructura.
        // Esto excluye a supervisores (Pastor/Coordinador) que no lideran un grupo personal,
        // pero mantiene a los discipuladores nuevos o sin reportes en el árbol.
        return hasSheep || isDiscipuladorRole;
    },

    getYearModules(year) {
        const modules = [];
        let currentSat = new Date(year, 0, 1);
        while (currentSat.getDay() !== 6) currentSat.setDate(currentSat.getDate() + 1);
        currentSat.setDate(currentSat.getDate() + 7); // Inicio Semana 1 Módulo 1

        for (let i = 1; i <= 4; i++) {
            const startSat = new Date(currentSat);
            const endSat = new Date(currentSat);
            endSat.setDate(endSat.getDate() + (11 * 7)); // 12 semanas

            modules.push({
                id: `m${i}`,
                name: `Módulo ${i}`,
                startSat: new Date(startSat),
                endSat: new Date(endSat)
            });

            currentSat = new Date(endSat);
            currentSat.setDate(currentSat.getDate() + 7);
        }
        return modules;
    },

    getModuleForDate(date) {
        const d = new Date(date);
        const diff = 6 - d.getDay();
        const repSat = new Date(d);
        repSat.setDate(repSat.getDate() + diff);
        repSat.setHours(0, 0, 0, 0);

        const modules = this.getYearModules(repSat.getFullYear());
        for (let m of modules) {
            const mS = new Date(m.startSat); mS.setHours(0, 0, 0, 0);
            const mE = new Date(m.endSat); mE.setHours(0, 0, 0, 0);
            if (repSat >= mS && repSat <= mE) return m;
        }
        return null;
    },

    getAvailableWeeks() {
        const today = new Date();
        const freeWeeks = (this.state.cache.metadata && this.state.cache.metadata.freeWeeks)
            ? this.state.cache.metadata.freeWeeks.map(fw => fw.saturday_date)
            : [];

        const weeks = [];
        // Calcular el Sábado de la semana actual
        const currentSat = new Date(today);
        const day = today.getDay();
        const diff = 6 - day;
        currentSat.setDate(today.getDate() + diff);
        currentSat.setHours(0, 0, 0, 0);

        // Generar las últimas 12 semanas para permitir ponerse al día con reportes atrasados
        // sin importar si pertenecen a un módulo anterior o si estamos en semana de transición.
        for (let i = 0; i < 12; i++) {
            const sat = new Date(currentSat);
            sat.setDate(currentSat.getDate() - (i * 7));

            const dateStr = sat.toISOString().split('T')[0];
            const fri = new Date(sat);
            fri.setDate(sat.getDate() - 1);

            weeks.push({
                friday: fri,
                saturday: sat,
                dateStr: dateStr,
                isFree: freeWeeks.includes(dateStr)
            });
        }

        // Ordenar de más reciente a más antiguo (opcional, el selector los agrupa por mes luego)
        return weeks.sort((a, b) => b.saturday - a.saturday);
    },

    getReportWeekRange(dateStr) {
        // En este sistema, la semana de reporte culmina en Sábado.
        // Calculamos el Viernes y Sábado correspondientes a la fecha seleccionada.
        const date = new Date(dateStr + 'T12:00:00'); // Evitar problemas de timezone
        const day = date.getDay(); // 0(Dom) - 6(Sab)

        // Distancia al Sábado (6)
        const diffToSaturday = 6 - day;
        const saturday = new Date(date);
        saturday.setDate(date.getDate() + diffToSaturday);

        const friday = new Date(saturday);
        friday.setDate(saturday.getDate() - 1);

        return { friday, saturday };
    },

    getReportStatus(dateStr, completeness = 100) {
        const freeWeeks = (this.state.cache.metadata && this.state.cache.metadata.freeWeeks)
            ? this.state.cache.metadata.freeWeeks.map(fw => fw.saturday_date)
            : [];

        if (freeWeeks.includes(dateStr)) {
            return { code: 'FREE', label: '🏖️ Semana Libre', color: '#10B981', canSend: false, msg: 'Esta semana ha sido configurada como semana libre. No se requiere reporte.' };
        }

        const { saturday } = this.getReportWeekRange(dateStr);
        const today = new Date();
        today.setHours(0, 0, 0, 0);

        const satCopy = new Date(saturday);
        satCopy.setHours(0, 0, 0, 0);

        // 1. Verificar si es semana futura (más allá de la actual)
        const available = this.getAvailableWeeks().map(w => w.dateStr);
        if (!available.includes(dateStr)) {
            const mondayCurrent = new Date();
            mondayCurrent.setDate(today.getDate() - ((today.getDay() + 6) % 7));
            mondayCurrent.setHours(0, 0, 0, 0);

            const mondayReport = new Date(satCopy);
            mondayReport.setDate(satCopy.getDate() - 5);
            mondayReport.setHours(0, 0, 0, 0);

            if (mondayReport > mondayCurrent) {
                return { code: 'LOCKED', label: '🔒 No disponible', color: '#94a3b8', canSend: false, msg: `El envío se habilita el ${this.formatFriendlyDate(saturday)}` };
            }
            return { code: 'LOCKED', label: '🔒 Fuera de rango', color: '#94a3b8', canSend: false, msg: 'Esta fecha ya no está disponible para reporte.' };
        }

        // 2. Verificar si es la semana actual pero aún no es sábado
        if (satCopy > today) {
            return { code: 'LOCKED', label: '🔒 No disponible', color: '#94a3b8', canSend: false, msg: `Podrás enviar este reporte a partir de mañana ${this.formatSimpleDay(saturday)}.` };
        }

        // 3. El sábado ya pasó o es hoy (Semana disponible y activa)
        if (completeness < 100) {
            return { code: 'INCOMPLETE', label: '⚠️ Incompleto', color: '#fca5a5', canSend: false, msg: 'Debes completar todos los registros antes de enviar' };
        }

        return { code: 'READY', label: '✅ Listo para enviar', color: '#10b981', canSend: true, msg: '' };
    },

    formatFriendlyDate(date) {
        return date.toLocaleDateString('es-ES', { weekday: 'long', day: 'numeric', month: 'long' });
    },

    formatSimpleDay(date) {
        const day = date.toLocaleDateString('es-ES', { weekday: 'long' });
        const num = date.getDate();
        const month = date.toLocaleDateString('es-ES', { month: 'long' });
        return `${day} ${num} de ${month}`;
    },

    // --- HELPERS ---
    getSectorMap(sectors, profiles, people) {
        const sectorMap = {};
        const unassignedSheep = []; // Personas sin discipulador o con responsable sin sector

        sectors.forEach(s => {
            sectorMap[s.id] = { sector: s, coordinator: null, disciples: [], coordSheep: [] };
        });

        const activeLeaderIdsInSectors = new Set();

        profiles.forEach(p => {
            if (!p.sector_id || !sectorMap[p.sector_id]) return;
            const entry = sectorMap[p.sector_id];
            const allRoles = [p.role, ...(p.additional_roles || [])];

            activeLeaderIdsInSectors.add(p.id);

            if (entry.sector.coordinator_id === p.id) {
                entry.coordinator = p;
            } else if (allRoles.includes('discipulador')) {
                entry.disciples.push({ profile: p, sheep: [] });
            } else if (allRoles.includes('coordinador') && !entry.coordinator) {
                entry.coordinator = p;
            }
        });

        people.forEach(person => {
            if (!person.assigned_to) {
                unassignedSheep.push(person);
                return;
            }

            let found = false;
            for (const sid in sectorMap) {
                const disciple = sectorMap[sid].disciples.find(d => d.profile.id === person.assigned_to);
                if (disciple) {
                    disciple.sheep.push(person);
                    found = true;
                    break;
                }
                if (sectorMap[sid].coordinator && sectorMap[sid].coordinator.id === person.assigned_to) {
                    sectorMap[sid].coordSheep.push(person);
                    found = true;
                    break;
                }
            }

            // Si tiene discipulador pero el responsable no está en la estructura de sectores (ej. es un Pastor Global)
            // NO lo metemos en unassignedSheep (porque ya está asignado), pero lo marcamos como 'found' 
            // para que no aparezca en pendientes.
            if (!found) {
                // Verificamos si el discipulador existe en la lista de perfiles general
                const leaderExists = profiles.some(p => p.id === person.assigned_to);
                if (!leaderExists) {
                    unassignedSheep.push(person); // Discipulador inexistente = huérfano
                }
            }
        });

        return { sectorMap, unassignedSheep };
    },

    // --- VISTAS ---
    views: {
        login() {
            return `
                <div class="login-container" style="background: radial-gradient(circle at top right, #1e293b, #0f172a);">
                    <div class="login-card-premium animate-reveal">
                        <div class="login-brand">
                            <img src="./assets/icons/icon-512x512.png" alt="Logo Elim" style="width:100px; height:100px; margin-bottom:20px; border-radius:24px; box-shadow:0 15px 30px rgba(0,0,0,0.1);">
                            <h1 style="color:var(--primary);">Elim App</h1>
                            <p style="color:var(--text-muted);">Gestión de Discipulado Profesional</p>
                        </div>
                        <form id="loginForm" class="modern-form" onsubmit="event.preventDefault();">
                            <div class="form-group">
                                <label style="color:var(--text-muted); font-size:12px; font-weight:700;">USUARIO O CORREO</label>
                                <div class="input-wrapper" style="position:relative;">
                                    <span class="input-icon" style="position:absolute; left:16px; top:50%; transform:translateY(-50%); opacity:0.5;">📧</span>
                                    <input type="email" id="username" placeholder="tu@iglesia.com" required autocapitalize="none" class="form-control" style="padding-left:48px; height:56px; border-radius:14px; background:#f8fafc;">
                                </div>
                            </div>
                            <div class="form-group" style="margin-top:20px;">
                                <label style="color:var(--text-muted); font-size:12px; font-weight:700;">CONTRASEÑA</label>
                                <div class="input-wrapper" style="position:relative;">
                                    <span class="input-icon" style="position:absolute; left:16px; top:50%; transform:translateY(-50%); opacity:0.5;">🔒</span>
                                    <input type="password" id="password" placeholder="••••••••" required class="form-control" style="padding-left:48px; height:56px; border-radius:14px; background:#f8fafc;">
                                </div>
                            </div>
                            <button type="submit" id="loginBtn" class="btn btn-primary" style="height:60px; margin-top:24px; font-size:18px; border-radius:16px;">
                                <span id="loginBtnText">Iniciar Sesión</span>
                                <span style="margin-left:8px;">→</span>
                            </button>
                        </form>
                        <div class="login-footer" style="text-align:center; margin-top:32px;">
                            <p style="font-size:11px; color:var(--text-muted); opacity:0.6;">© ${new Date().getFullYear()} Iglesia Elim  •  Diseño Profesional v3.0</p>
                        </div>
                    </div>
                </div>
            `;
        },

        errorState(msg, isOffline = false) {
            const title = isOffline ? 'Sin conexión a internet' : 'Problema de Conexión';
            const icon = isOffline ? '📡' : '☁️';
            const message = isOffline
                ? 'No pudimos conectarnos al sistema. Verifica tu conexión e intenta nuevamente.'
                : 'Hubo un inconveniente al intentar sincronizar con el servidor de base de datos.';

            return `
                <div class="status-screen-overlay animate-reveal">
                    <div class="status-card glass">
                        <div class="status-icon-container float-animation">
                            <span class="status-icon-premium">${icon}</span>
                        </div>
                        
                        <h2 class="status-title-premium">${title}</h2>
                        
                        <p class="status-message-premium">
                            ${message}
                        </p>
                        
                        <div class="status-actions">
                            <button id="retryBtn" class="btn btn-primary status-retry-btn-premium" onclick="this.disabled=true; this.innerHTML='<span class=\\'spinner-sm\\'></span> Reintentando...'; setTimeout(() => App.router(), 800);">
                                Reintentar
                            </button>
                        </div>
                        
                        <div class="status-footer-hint">
                            <p>El sistema intentará reconectar automáticamente al detectar señal.</p>
                        </div>
                    </div>
                </div>
            `;
        },

        loadingState(message = 'Cargando contenido...') {
            return `
                <div class="view-container animate-reveal" style="padding:100px 24px; text-align:center;">
                    <div class="spinner-premium" style="margin: 0 auto 24px; width:48px; height:48px; border:4px solid rgba(0,0,0,0.05); border-top-color:var(--accent); border-radius:50%; animation: spin 0.8s linear infinite;"></div>
                    <h3 style="color:var(--text-muted); font-size:16px; font-weight:600;">${message}</h3>
                    <p style="color:var(--text-muted); opacity:0.5; font-size:12px; margin-top:8px;">Por favor espera un momento</p>
                </div>
            `;
        },

        modeSelector() {
            const user = App.state.user;
            const roleConfig = {
                coordinador: {
                    icon: '🗂️',
                    label: 'Coordinador',
                    desc: 'Gestiona la estructura de discipuladores y supervisa el avance de su sector.',
                    color: '#7C3AED'
                },
                discipulador: {
                    icon: '📝',
                    label: 'Discipulador',
                    desc: 'Realiza el pase de asistencia semanal y envía reportes de seguimiento.',
                    color: '#2563EB'
                },
                pastor: {
                    icon: '⛪',
                    label: 'Pastor',
                    desc: 'Supervisión global de la iglesia, sectores y administración total de usuarios.',
                    color: '#059669'
                }
            };

            const roleButtons = user.allRoles.map(role => {
                const cfg = roleConfig[role] || { icon: '👤', label: role, desc: 'Acceso a funciones del sistema.', color: '#64748B' };
                return `
                    <button class="mode-btn" data-mode="${role}" style="--mode-color: ${cfg.color}; height: auto; align-items: flex-start; padding: 20px;">
                        <span class="mode-icon" style="margin-top: 4px;">${cfg.icon}</span>
                        <div class="mode-text">
                            <h3 style="margin-bottom: 4px;">${cfg.label}</h3>
                            <p style="font-size: 13px; color: var(--text-muted); line-height: 1.4;">${cfg.desc}</p>
                        </div>
                        <span class="mode-arrow" style="align-self: center;">›</span>
                    </button>
                `;
            }).join('');

            return `
                <div class="mode-selector-container">
                    <div class="mode-selector-card glass" style="max-width: 480px; padding: 40px 32px;">
                        <div class="mode-header" style="text-align: center; margin-bottom: 32px;">
                            <div class="sector-avatar" style="margin: 0 auto 16px; width: 64px; height: 64px; font-size: 24px; background: linear-gradient(135deg, var(--accent), #7C3AED);">${user.name.charAt(0)}</div>
                            <h2 style="font-size: 24px;">Bienvenido, ${user.name}</h2>
                            <p style="color: var(--text-muted); margin-top: 8px;">Selecciona el rol que deseas ejercer hoy:</p>
                        </div>
                        <div class="mode-options" style="display: flex; flex-direction: column; gap: 16px; margin-bottom: 32px;">${roleButtons}</div>
                        <button class="logout-link" id="modeSelectorLogout" style="font-weight: 700; color: var(--danger); opacity: 0.8;">Cerrar sesión de forma segura</button>
                    </div>
                </div>
            `;
        },

        dashboard(stats = { attendance: '--', reports: '--', pendingCount: 0, isFreeWeek: false }) {
            const user = App.state.user;
            const activeMode = App.state.activeMode || user.role;
            const isAdmin = user.role === 'pastor';
            const isCoord = activeMode === 'coordinador';

            const modeLabel = isCoord ? 'Coordinador' : (isAdmin ? 'Pastor' : 'Discipulador');
            const modeColor = isCoord ? 'var(--primary-alt)' : (isAdmin ? '#059669' : '#2563EB');

            const isFriday = new Date().getDay() === 5;

            let actionsHtml = '';
            // ... (rest of actions logic remains same but we'll include it in the replace block)
            if (activeMode === 'discipulador') {
                if (stats.isReporter !== false) {
                    actionsHtml += `
                        <a href="#/reporte" class="action-card">
                            <div class="action-card-icon" style="background:rgba(79,106,254,0.1); color:var(--primary-alt);">📝</div>
                            <div class="action-card-text"><h4>Crear Reporte</h4><p>Pasar asistencia semanal</p></div>
                        </a>
                    `;
                }
                actionsHtml += `
                    <a href="#/historial" class="action-card">
                        <div class="action-card-icon" style="background:rgba(99,102,241,0.1); color:#6366f1;">📋</div>
                        <div class="action-card-text"><h4>Historial</h4><p>Ver reportes enviados</p></div>
                    </a>`;
            }
            if (isCoord) {
                actionsHtml += `
                    <a href="#/estructura" class="action-card">
                        <div class="action-card-icon" style="background:rgba(124,58,237,0.1); color:#7C3AED;">👥</div>
                        <div class="action-card-text"><h4>Mi Estructura</h4><p>Gestionar discipuladores</p></div>
                    </a>`;
            }
            actionsHtml += `
                <a href="#/estadisticas" class="action-card">
                    <div class="action-card-icon" style="background:rgba(245,158,11,0.1); color:#F59E0B;">📊</div>
                    <div class="action-card-text"><h4>Estadísticas</h4><p>Análisis de asistencia</p></div>
                </a>`;
            if (isAdmin) {
                actionsHtml += `
                    <a href="#/admin" class="action-card">
                        <div class="action-card-icon" style="background:rgba(5,150,105,0.1); color:#059669;">⚙️</div>
                        <div class="action-card-text"><h4>Administración</h4><p>Ajustes globales</p></div>
                    </a>`;
                actionsHtml += `
                    <a href="#/reporte-sector" class="action-card">
                        <div class="action-card-icon" style="background:rgba(124,58,237,0.1); color:#7C3AED;">📊</div>
                        <div class="action-card-text"><h4>Reporte por Sectores</h4><p>Resumen semanal por sector</p></div>
                    </a>`;
            } else if (isCoord) {
                actionsHtml += `
                    <a href="#/reporte-sector" class="action-card">
                        <div class="action-card-icon" style="background:rgba(124,58,237,0.1); color:#7C3AED;">📊</div>
                        <div class="action-card-text"><h4>Reporte por Sector</h4><p>Resumen semanal de asistencia</p></div>
                    </a>`;
            }

            // Alerta de reportes pendientes
            const pendingAlertHtml = stats.pendingCount > 0 ? `
                <div class="glass" style="background: linear-gradient(135deg, #FFFBEB 0%, #FEF3C7 100%); border: 1px solid #FCD34D; padding: 20px; border-radius: 20px; display: flex; align-items: center; gap: 16px; margin-bottom: 24px;">
                    <span style="font-size: 32px;">⚠️</span>
                    <div>
                        <h4 style="color: #92400E; margin: 0 0 4px 0;">Atención Discipulador</h4>
                        <p style="color: #B45309; font-size: 14px; margin: 0;">Tienes <strong>${stats.pendingCount} reportes pendientes</strong> de envío. Por favor, complétalos a la brevedad.</p>
                    </div>
                </div>
            ` : '';

            return `
                <div class="view-container">
                    <header class="header dashboard-header-pc">
                        <div class="header-user">
                            <p style="font-weight:700; text-transform:uppercase; font-size:10px; letter-spacing:0.15em; margin-bottom:4px; opacity: 0.8;">Resumen General</p>
                            <h2 style="">Dios te bendiga, ${user.name}</h2>
                        </div>
                        <div class="header-actions">
                            ${user.allRoles.length > 1 ? `
                                <button class="btn-role-switcher" id="switchModeBtn">
                                    <span class="icon-badge">⇄</span>
                                    <span class="btn-label">Cambiar Rol</span>
                                </button>
                            ` : ''}
                            <button id="logoutBtn" class="btn-logout">Salir</button>
                        </div>
                    </header>

                    <div class="dashboard-content">
                        ${stats.isFreeWeek ? `
                            <div class="glass status-banner-success animate-reveal">
                                <div class="status-banner-icon">🏖️</div>
                                <div class="status-banner-text">
                                    <h4>Semana de Descanso</h4>
                                    <p>No se requiere el envío de reportes para esta semana.</p>
                                </div>
                            </div>
                        ` : ''}

                        ${!stats.isFreeWeek ? pendingAlertHtml : ''}
                        
                        ${!stats.isFreeWeek && isFriday ? `
                            <div class="glass status-banner-info">
                                <div class="status-banner-icon">📅</div>
                                <div class="status-banner-text">
                                    <h4>Recordatorio de Viernes</h4>
                                    <p>Tienes un reporte disponible para mañana sábado.</p>
                                </div>
                            </div>
                        ` : ''}

                        ${stats.isExec ? `
                            <div class="metric-card executive-summary-card animate-reveal">
                                <div class="card-header-flex">
                                    <h3>Análisis Semanal</h3>
                                    <span class="live-badge">En vivo</span>
                                </div>
                                
                                <div class="executive-metrics-grid">
                                    <div class="exec-metric sent">
                                        <div class="val">${stats.reportsSent === undefined ? '--' : stats.reportsSent}</div>
                                        <div class="label">Reportes Hechos</div>
                                    </div>
                                    <div class="exec-metric missing">
                                        <div class="val">${stats.missingReports === undefined ? '--' : stats.missingReports}</div>
                                        <div class="label">Faltantes</div>
                                    </div>
                                </div>

                                <div class="attendance-breakdown-row">
                                    <div class="day-stat">
                                        <div class="val">${stats.attFriday === undefined ? '--' : stats.attFriday} <span>ovejas</span></div>
                                        <div class="label">Viernes</div>
                                    </div>
                                    <div class="day-stat">
                                        <div class="val">${stats.attSaturday === undefined ? '--' : stats.attSaturday} <span>ovejas</span></div>
                                        <div class="label">Sábado</div>
                                    </div>
                                </div>
                            </div>
                            
                            ${stats.repeatedAbsences && stats.repeatedAbsences.length > 0 ? `
                                <div class="glass warning-intelligence-card animate-reveal">
                                    <h4><span>⚠️</span> Inteligencia Pastoral</h4>
                                    <p>Ovejas con ausencias consecutivas:</p>
                                    <div class="intelligence-list">
                                        ${stats.repeatedAbsences.map(s => `
                                            <div class="intelligence-item">
                                                <span class="name">${s.name}</span>
                                                <span class="count-badge">${s.count} semanas</span>
                                            </div>
                                        `).join('')}
                                    </div>
                                    <div class="card-footer-link">
                                        <a href="#/estadisticas">Ver análisis completo <span class="footer-arrow">→</span></a>
                                    </div>
                                </div>
                            ` : ''}
                        ` : `
                            <div class="metrics-grid">
                                <div class="metric-card">
                                    <h3>${stats.attendance || '--'}</h3>
                                    <p>Asistencia Promedio</p>
                                </div>
                                <div class="metric-card">
                                    <h3>${stats.reports || '--'}</h3>
                                    <p>Reportes Enviados</p>
                                </div>
                            </div>
                        `}

                        <div class="section-group">
                            <h3 class="section-title">Accesos Rápidos <span class="mode-badge">${modeLabel}</span></h3>
                            <div class="action-grid">${actionsHtml}</div>
                        </div>

                        ${!stats.isExec ? `
                        <div style="margin-top:40px;">
                            <h3 class="section-title" style="margin-bottom:20px;">Detalles de la Estructura</h3>
                            <div class="data-table-container">
                                <div class="data-row" style="background:#F8FAFC; border-radius:16px 16px 0 0; font-size:11px; font-weight:800; color:var(--text-muted); text-transform:uppercase;">
                                    <span>Sector / Discipulador</span>
                                    <span>Estado General</span>
                                    <span>Rendimiento</span>
                                    <span>Acción</span>
                                </div>
                                <div class="data-row">
                                    <div class="data-info">
                                        <div class="sector-avatar sm" style="background:var(--primary-alt);">1</div>
                                        <div><div class="data-title">${user.sector}</div><div class="data-subtitle">Discipulador: ${user.name}</div></div>
                                    </div>
                                    <div class="progress-mini-wrapper">
                                        <div class="progress-mini-bar"><div class="progress-mini-fill" style="width:${stats.attendance}; background:var(--primary-alt);"></div></div>
                                        <div style="font-size:10px; font-weight:700; color:var(--text-muted);">Eficiencia de reporte</div>
                                    </div>
                                    <div style="font-weight:700; color:var(--primary); font-size: 13px;">Óptimo</div>
                                    <div><a href="#/reporte" class="pill pill-success" style="font-size:10px; padding:6px 10px; text-decoration:none;">IR A REPORTE</a></div>
                                </div>
                            </div>
                        </div>
                        ` : ''}
                    </div>
                </div>
            `;
        },

        report(people, dateStr, weeks) {
            const { friday, saturday } = App.getReportWeekRange(dateStr);
            const status = App.getReportStatus(dateStr, 0);
            const isFree = status.code === 'FREE';

            const groupedWeeks = {};
            weeks.forEach(w => {
                const monthName = w.saturday.toLocaleDateString('es-ES', { month: 'long' });
                const year = w.saturday.getFullYear();
                const key = `${monthName} ${year}`;
                if (!groupedWeeks[key]) groupedWeeks[key] = [];
                groupedWeeks[key].push(w);
            });

            const weekOptions = Object.entries(groupedWeeks).map(([monthYear, monthWeeks]) => `
                <optgroup label="${monthYear.toUpperCase()}">
                    ${monthWeeks.map(w => `
                        <option value="${w.dateStr}" ${w.dateStr === dateStr ? 'selected' : ''}>
                            Del ${w.friday.getDate()} al ${w.saturday.getDate()} — Asistencia
                        </option>
                    `).join('')}
                </optgroup>
            `).join('');

            const headerHtml = `
                <header class="header" style="z-index: 1001;">
                    <div class="header-user">
                        <p style="font-weight:700; text-transform:uppercase; font-size:10px; letter-spacing:0.15em; margin-bottom:4px; opacity: 0.8;">Sector: ${App.state.user.sector}</p>
                        <h2 style="">Pase de Lista</h2>
                    </div>
                    <a href="#/dashboard" class="btn-back-modern">
                        <span>←</span>
                        Volver
                    </a>
                </header>
                
                <div class="report-sticky-info glass">
                    <div class="sticky-flex">
                        <div class="sticky-date">
                            <span class="label">Semana</span>
                            <span class="val">${App.formatSimpleDay(friday).split(' de ')[0]} - ${saturday.getDate()} ${saturday.toLocaleDateString('es-ES', { month: 'short' })}</span>
                        </div>
                        <div class="sticky-stats">
                            <div class="stat-item">
                                <span class="label">Progreso</span>
                                <span class="val" id="stickyProgressText">0%</span>
                            </div>
                        </div>
                    </div>
                    <div class="sticky-progress-bar">
                        <div id="stickyProgressBar" class="fill" style="width: 0%"></div>
                    </div>
                </div>
            `;

            const contextCardHtml = `
                <div class="context-card animate-reveal" style="margin-top: 10px;">
                    <div class="selector-wrapper-modern" style="position:relative;">
                        <div style="position:absolute; left:14px; top:-10px; background:white; padding:0 8px; font-size:11px; font-weight:800; color:#3b82f6; z-index:10; border-radius:4px; pointer-events:none;">
                            SELECTOR DE SEMANA
                        </div>
                        <select id="reportDate" class="week-selector-modern" style="border: 2px solid #f1f5f9; padding-top:16px; padding-bottom:16px; height:auto; position:relative; z-index:5;">
                            ${weekOptions}
                        </select>
                    </div>
                </div>
            `;

            const listHtml = people.length === 0 ?
                '<div style="padding:40px; text-align:center; color:#64748b;">No hay personas asignadas</div>' :
                `<div class="people-list-modern">
                    ${people.map(p => {
                    const nameParts = p.name.split(' ');
                    const avatarChars = (nameParts[0].charAt(0) + (nameParts[1] ? nameParts[1].charAt(0) : '')).toUpperCase();
                    return `
                        <div class="person-card-premium person-row-ref" data-id="${p.id}" data-fri="unanswered" data-sat="unanswered">
                            <div class="person-header-premium">
                                <div class="person-avatar-premium" style="width:36px; height:36px; font-size:12px;">${avatarChars}</div>
                                <div class="person-name-premium" style="font-size:14px;">${p.name}</div>
                            </div>
                            <div class="attendance-controls-premium">
                                <div class="day-control-block">
                                    <span class="day-label-mini">VIE</span>
                                    <div class="segmented-control">
                                        <button class="segmented-btn btn-fri-present present" data-val="present"><span>✓</span></button>
                                        <button class="segmented-btn btn-fri-absent absent" data-val="absent"><span>✕</span></button>
                                    </div>
                                </div>
                                <div class="day-control-block">
                                    <span class="day-label-mini">SÁB</span>
                                    <div class="segmented-control">
                                        <button class="segmented-btn btn-sat-present present" data-val="present"><span>✓</span></button>
                                        <button class="segmented-btn btn-sat-absent absent" data-val="absent"><span>✕</span></button>
                                    </div>
                                </div>
                            </div>
                        </div>`;
                }).join('')}
                </div>`;

            return `
                <div id="reportAppContainer" class="view-container report-view-premium">
                    ${headerHtml}
                    <div class="dashboard-content" style="padding: 0; padding-top: 24px; padding-bottom: 180px;">
                        ${contextCardHtml}
                        <div id="blockerMsg" style="margin: 0 20px 20px; display:${status.msg ? 'flex' : 'none'}; padding: 16px; background: #fff8eb; border: 1px solid #fee2e2; border-radius: 16px; gap: 12px; align-items: center; color: #92400e; font-size: 13px; font-weight: 600;">
                            <span>⚠️</span>
                            <span id="blockerText">${status.msg}</span>
                        </div>
                        ${isFree ? `
                            <div style="padding:60px 24px; text-align:center;">
                                <div style="font-size:64px; margin-bottom:16px;">🏖️</div>
                                <h3 style="color:#3b82f6; font-size:20px;">Semana Libre</h3>
                                <p style="color:#64748b; font-size:14px; margin-top:8px;">No se requiere reporte para esta fecha.</p>
                            </div>
                        ` : listHtml}
                    </div>
                    
                    ${!isFree ? `
                    <div class="bottom-actions-premium animate-reveal">
                        <div class="progress-premium-container">
                            <div class="progress-premium-text">
                                <span>PROGRESO</span>
                                <span id="progressText">0 / ${people.length}</span>
                            </div>
                            <div class="progress-premium-bar">
                                <div id="progressBar" class="progress-premium-fill" style="width: 0%"></div>
                            </div>
                        </div>
                        <div class="actions-buttons-premium">
                            <button id="saveDraftBtn" class="btn-premium-draft">Borrador</button>
                            <button id="sendReportBtn" class="btn-premium-send" disabled>Enviar Reporte</button>
                        </div>
                    </div>
                    ` : ''}
                </div>
            `;
        },

        historial(reports, meta = null, currentFilters = {}, hasMore = false) {
            const isPastor = App.state.user.role === 'pastor';
            let filterHtml = '';

            if (isPastor && meta) {
                // Filtros Premium (Igual que en Estadísticas)
                filterHtml = `
                    <div class="glass animate-reveal" style="margin-bottom:28px; padding:24px; border-radius:32px; border:1px solid var(--glass-border);">
                        <h3 class="section-title" style="margin-bottom:16px; font-size:14px; letter-spacing:0.05em; color:var(--text-muted);">Filtros de Búsqueda Histórica</h3>
                        <div class="stats-filter-grid">
                            <div class="filter-group">
                                <label style="font-size:10px; font-weight:800; color:var(--text-muted); text-transform:uppercase; margin-bottom:6px; display:block;">Sector</label>
                                <select id="fSector" class="form-control-premium">
                                    <option value="">Cualquier Sector</option>
                                    ${meta.sectors.map(s => `<option value="${s.id}" ${currentFilters.sector_id === s.id ? 'selected' : ''}>${s.name}</option>`).join('')}
                                </select>
                            </div>
                            <div class="filter-group">
                                <label style="font-size:10px; font-weight:800; color:var(--text-muted); text-transform:uppercase; margin-bottom:6px; display:block;">Discipulador</label>
                                <select id="fLeader" class="form-control-premium">
                                    <option value="">Cualquier Discipulador</option>
                                    ${meta.discipliners.map(l => `<option value="${l.id}" ${currentFilters.reporter_id === l.id ? 'selected' : ''}>${l.name}</option>`).join('')}
                                </select>
                            </div>
                            <div class="filter-group">
                                <label style="font-size:10px; font-weight:800; color:var(--text-muted); text-transform:uppercase; margin-bottom:6px; display:block;">Oveja</label>
                                <select id="fSheep" class="form-control-premium">
                                    <option value="">Todas las Personas</option>
                                    ${meta.sheep.map(p => `<option value="${p.id}" ${currentFilters.person_id === p.id ? 'selected' : ''}>${p.name}</option>`).join('')}
                                </select>
                            </div>
                            <div class="filter-group" style="display:flex; align-items:flex-end;">
                                <button id="btnClearFilters" class="btn" style="width:100%; background:rgba(0,0,0,0.05); color:var(--text-muted); height:46px; border-radius:14px; font-size:12px; font-weight:700;">Limpiar Filtros</button>
                            </div>
                        </div>
                    </div>`;
            }

            // Detectar fechas duplicadas DE MANERA AISLADA (por usuario)
            const dateFreq = {};
            reports.forEach(r => {
                const uniqueKey = `${r.reporter_id}_${r.report_date}`;
                dateFreq[uniqueKey] = (dateFreq[uniqueKey] || 0) + 1;
            });

            const renderCard = (r) => {
                const date = new Date(r.report_date + 'T12:00:00');
                const formattedDate = App.formatFriendlyDate(date);
                const attList = r.attendances || [];
                const attCount = attList.length;
                const reporterName = r.reporter ? r.reporter.name : 'Desconocido';

                const uniqueKey = `${r.reporter_id}_${r.report_date}`;
                const isDuplicate = dateFreq[uniqueKey] > 1;

                const lId = r.reporter_id;
                const leaderNode = meta ? meta.discipliners.find(d => d.id === lId) : null;
                const sId = leaderNode ? leaderNode.sector_id : (r.reporter?.sector_id || '');

                const attHtml = attList.map((a, idx) => `
                    <div class="person-row-item" style="display:flex; justify-content:space-between; padding:12px 0; ${idx === attList.length - 1 ? '' : 'border-bottom:1px solid rgba(0,0,0,0.03);'}">
                        <span style="font-weight:600; color:#475569; font-size:14px;">${a.people_assigned ? a.people_assigned.name : 'Discípulo sin nombre'}</span>
                        <div style="display:flex; gap:8px;">
                            <span class="pill ${a.attended_friday ? 'pill-success' : 'pill-danger'}" style="width:28px; height:28px; display:inline-block; line-height:28px; text-align:center; border-radius:50%; font-size:10px; font-weight:900; padding:0 !important; box-shadow: 0 3px 6px rgba(0,0,0,0.08);">V</span>
                            <span class="pill ${a.attended_saturday ? 'pill-success' : 'pill-danger'}" style="width:28px; height:28px; display:inline-block; line-height:28px; text-align:center; border-radius:50%; font-size:10px; font-weight:900; padding:0 !important; box-shadow: 0 3px 6px rgba(0,0,0,0.08);">S</span>
                        </div>
                    </div>`).join('');

                return `
                    <div class="history-card glass ${isDuplicate ? 'duplicate-warning' : ''} _pastor-card" data-report-id="${r.id}" data-sector-id="${sId}" data-leader-id="${lId}" data-module-id="${r.moduleId || 'out'}" style="border-radius:24px; padding:24px; margin-bottom:20px; border-left: 6px solid ${isDuplicate ? 'var(--danger)' : 'rgba(37,99,235,0.4)'}; background: ${isDuplicate ? '#FFF1F2' : 'white'}; position:relative; box-shadow: 0 4px 20px rgba(0,0,0,0.02); ${isPastor ? 'cursor: pointer;' : ''}" ${isPastor ? 'title="Doble clic para ver detalle completo"' : ''}>
                        ${isDuplicate ? `<div style="position:absolute; top:12px; right:24px; font-size:9px; font-weight:900; color:var(--danger); background:white; padding:2px 8px; border-radius:20px; border:1px solid var(--danger);">FECHA REPETIDA</div>` : ''}
                        
                        <div style="border-bottom:1px solid #f1f5f9; padding-bottom:18px; margin-bottom:18px; display:flex; justify-content:space-between; align-items:center;">
                            <div style="display:flex; align-items:center; gap:14px;">
                                <div style="background:rgba(37,99,235,0.08); color:var(--primary); width:48px; height:48px; border-radius:14px; display:flex; align-items:center; justify-content:center; font-size:20px; flex-shrink:0;">📅</div>
                                <div>
                                    <h4 style="margin:0; font-size:17px; font-weight:800; color:#1e293b;">${formattedDate}</h4>
                                    <div style="font-size:11px; color:#64748b; margin-top:3px; text-transform:uppercase; letter-spacing:0.05em;">Responsable: <span style="color:var(--primary); font-weight:800;">${reporterName}</span></div>
                                </div>
                            </div>
                            <div style="text-align:right;">
                                <div style="font-size:13px; font-weight:800; color:${attCount === 0 ? '#ef4444' : '#1e293b'};">${attCount} Ovejas ${attCount === 0 ? '(Vacío)' : ''}</div>
                                <div style="margin-top:8px; display:flex; gap:6px; justify-content:flex-end;">
                                    ${isPastor ? `<button class="btn-delete-report" data-id="${r.id}" style="background:none; border:none; color:#ef4444; font-size:10px; font-weight:800; cursor:pointer; padding:6px 8px; border-radius:8px; transition:all 0.2s;">ELIMINAR</button>` : ''}
                                    <button class="btn-download-pdf-direct" data-id="${r.id}" style="background:#ecfdf5; border:1px solid #d1fae5; color:#059669; font-size:10px; font-weight:800; cursor:pointer; padding:6px 10px; border-radius:10px; transition:all 0.2s;"><span class="btn-text">📄 PDF</span></button>
                                    <button class="btn-edit-report" data-id="${r.id}" style="background:#eff6ff; border:1px solid #dbeafe; color:#2563eb; font-size:10px; font-weight:800; cursor:pointer; padding:6px 10px; border-radius:10px; transition:all 0.2s;">✏️ EDITAR</button>
                                </div>
                            </div>
                        </div>
                        <div class="history-attendances" style="max-height: 200px; overflow-y: auto; padding-right:8px;">
                            ${attHtml}
                        </div>
                    </div>`;
            };

            let innerContentHtml = '';

            if (isPastor && meta) {
                // Calculate modules
                const currentYear = new Date().getFullYear();

                const modules = [];
                let currentSat = new Date(currentYear, 0, 1);
                while (currentSat.getDay() !== 6) {
                    currentSat.setDate(currentSat.getDate() + 1);
                }
                currentSat.setDate(currentSat.getDate() + 7); // Segundo Sábado (Fin semana 1 Módulo 1)

                for (let i = 1; i <= 4; i++) {
                    const startSat = new Date(currentSat);
                    const endSat = new Date(currentSat);
                    endSat.setDate(endSat.getDate() + (11 * 7)); // 12 sábados exactos componen el módulo

                    const displayStart = new Date(startSat);
                    displayStart.setDate(displayStart.getDate() - 6); // Domingo (inicio visual)

                    modules.push({
                        id: `m${i}`,
                        name: `Módulo ${i}`,
                        startSat: new Date(startSat),
                        endSat: new Date(endSat),
                        startStr: displayStart.toLocaleDateString('es-ES', { day: '2-digit', month: 'short' }),
                        endStr: endSat.toLocaleDateString('es-ES', { day: '2-digit', month: 'short' })
                    });

                    currentSat = new Date(endSat);
                    currentSat.setDate(currentSat.getDate() + 7); // Siguiente semana inicia sgte Sábado
                }

                // Filter reports to current year based on their explicit week
                const yearReports = reports.filter(r => {
                    const d = new Date(r.report_date + 'T12:00:00');
                    const diff = 6 - d.getDay();
                    const repSat = new Date(d);
                    repSat.setDate(repSat.getDate() + diff);
                    return repSat.getFullYear() === currentYear;
                });

                // Classify by exact module weeks
                yearReports.forEach(r => {
                    const d = new Date(r.report_date + 'T12:00:00');
                    const diff = 6 - d.getDay();
                    const repSat = new Date(d);
                    repSat.setDate(repSat.getDate() + diff);
                    repSat.setHours(0, 0, 0, 0);

                    r.moduleId = 'out';
                    for (let m of modules) {
                        const mS = new Date(m.startSat); mS.setHours(0, 0, 0, 0);
                        const mE = new Date(m.endSat); mE.setHours(0, 0, 0, 0);

                        if (repSat >= mS && repSat <= mE) {
                            r.moduleId = m.id;
                            break;
                        }
                    }
                });

                // Calculate initial tree counts (for 'all' default) excluding 'out'
                const activeYearReports = yearReports.filter(r => r.moduleId !== 'out');

                const counts = { sectors: {}, leaders: {}, total: activeYearReports.length };
                activeYearReports.forEach(r => {
                    const lId = r.reporter_id;
                    const leaderNode = meta.discipliners.find(d => d.id === lId);
                    const sId = leaderNode ? leaderNode.sector_id : (r.reporter?.sector_id || '');
                    if (sId) counts.sectors[sId] = (counts.sectors[sId] || 0) + 1;
                    if (lId) counts.leaders[lId] = (counts.leaders[lId] || 0) + 1;
                });

                let tabsHtml = `
                    <div style="margin-bottom: 24px;">
                        <h3 style="font-size:18px; font-weight:800; color:var(--text-main); margin-bottom:12px; display:flex; align-items:center; gap:8px;">
                            <span>📅 Año <span style="color:var(--primary);">${currentYear}</span></span>
                            <span style="font-size:11px; background:rgba(37,99,235,0.1); color:var(--primary); padding:4px 10px; border-radius:12px;">Historial Activo</span>
                        </h3>
                        <div class="module-tabs" style="display:flex; gap:8px; overflow-x:auto; padding-bottom:12px; scrollbar-width: none;">
                `;

                tabsHtml += `
                    <button class="btn btn-module active" data-module="all" style="width: auto; flex-shrink:0; border-radius:14px; padding:10px 16px; font-size:13px; font-weight:700; border:1px solid transparent; background:var(--primary); color:white; transition:all 0.2s; white-space:nowrap; box-shadow:0 4px 12px rgba(37,99,235,0.2);" onclick="if(window.ElimPastorUI) window.ElimPastorUI.changeModule(this, 'all', ${currentYear}, 'Todos los Módulos')">
                        Todos los Módulos
                    </button>
                `;

                modules.forEach(m => {
                    tabsHtml += `
                        <button class="btn btn-module" data-module="${m.id}" style="width: auto; flex-shrink:0; border-radius:14px; padding:10px 16px; font-size:13px; font-weight:700; border:1px solid rgba(0,0,0,0.1); background:white; color:var(--text-muted); transition:all 0.2s; white-space:nowrap;" onclick="if(window.ElimPastorUI) window.ElimPastorUI.changeModule(this, '${m.id}', ${currentYear}, '${m.name}')">
                            ${m.name} <span style="font-size:10px; opacity:0.7; font-weight:500; margin-left:6px;">(${m.startStr} - ${m.endStr})</span>
                        </button>
                    `;
                });

                tabsHtml += `
                    </div>
                </div>
                `;

                let treeHtml = `
                    <div class="glass" style="padding: 16px; border-radius: 20px; border: 1px solid var(--glass-border); max-height: calc(100vh - 200px); overflow-y: auto; position: sticky; top: 20px;">
                        <h4 style="margin:0 0 16px; font-size:13px; font-weight:800; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.05em; display:flex; justify-content:space-between; align-items:center;">
                            <span>📁 Explorador</span>
                        </h4>
                        <div class="folder-tree">
                            <div class="folder-item active" data-folder-type="all" style="padding: 12px; border-radius: 14px; cursor: pointer; display:flex; justify-content:space-between; align-items:center; background: rgba(37,99,235,0.1); color: var(--primary); font-weight:700; margin-bottom:12px; transition:all 0.2s;" onclick="if(window.ElimPastorUI) window.ElimPastorUI.filterRender('all')">
                                <div><span style="margin-right:8px; font-size:16px;">📂</span> Todos los Sectores</div>
                                <span class="pill tree-count-all" style="font-size:10px; font-weight:800; background:white; color:var(--primary); box-shadow:0 2px 4px rgba(0,0,0,0.05);">${counts.total}</span>
                            </div>
                `;

                meta.sectors.forEach(s => {
                    const sectorCount = counts.sectors[s.id] || 0;
                    const leaders = meta.discipliners.filter(l => l.sector_id === s.id && App.isReporter(l, meta));

                    treeHtml += `
                        <div class="folder-group" style="margin-bottom: 8px;">
                            <div class="folder-item" data-folder-type="sector" data-id="${s.id}" style="padding: 10px 12px; border-radius: 12px; cursor: pointer; display:flex; justify-content:space-between; align-items:center; color: var(--text-main); font-weight:600; transition: all 0.2s; background: transparent;" onclick="if(window.ElimPastorUI) window.ElimPastorUI.toggleFolder(this); if(window.ElimPastorUI) window.ElimPastorUI.filterRender('sector', '${s.id}', null)">
                                <div style="display:flex; align-items:center; gap:8px;">
                                    <span class="folder-icon" style="opacity:0.7; font-size:14px; transition:transform 0.2s;">📁</span> 
                                    <span style="font-size:14px;">${s.name}</span>
                                </div>
                                <span class="tree-count-sector" data-id="${s.id}" style="font-size:11px; color:var(--text-muted); font-weight:800; background:rgba(0,0,0,0.04); padding:2px 8px; border-radius:10px;">${sectorCount}</span>
                            </div>
                            <div class="folder-children" style="display: none; padding-left: 20px; margin-top: 4px; border-left: 2px solid rgba(0,0,0,0.03); margin-left: 17px; margin-bottom:8px;">
                    `;

                    leaders.forEach(l => {
                        const leaderCount = counts.leaders[l.id] || 0;
                        treeHtml += `
                            <div class="folder-item sub-folder" data-folder-type="leader" data-id="${l.id}" style="padding: 8px 10px; border-radius: 10px; cursor: pointer; display:flex; justify-content:space-between; align-items:center; color: var(--text-muted); font-size: 13px; font-weight:500; transition: all 0.2s; margin-bottom:2px;" onclick="if(window.ElimPastorUI) window.ElimPastorUI.filterRender('leader', '${l.id}', '${s.id}')">
                                <div style="display:flex; align-items:center; gap:6px;">
                                    <span style="opacity:0.4; font-size:12px;">↳</span>👤 <span>${l.name}</span>
                                </div>
                                <span class="tree-count-leader" data-id="${l.id}" style="font-size:10px; color:var(--text-muted); opacity:0.8; font-weight:700;">${leaderCount}</span>
                            </div>
                        `;
                    });

                    treeHtml += `
                            </div>
                        </div>
                    `;
                });

                treeHtml += `
                        </div>
                    </div>
                `;

                const rightPanelHtml = `
                    <div id="pastorReportsPanel" style="flex:1; min-width:0; display:flex; flex-direction:column;">
                        <div style="background: white; padding: 16px 24px; border-radius: 20px; border: 1px solid var(--border); box-shadow: var(--shadow-sm); margin-bottom: 20px; display:flex; align-items:center; gap:12px; flex-wrap:wrap;">
                            <div style="background:rgba(37,99,235,0.1); color:var(--primary); width:32px; height:32px; border-radius:10px; display:flex; align-items:center; justify-content:center; font-size:14px; flex-shrink:0;">📍</div>
                            <div id="pastorBreadcrumb" style="font-size:14px; font-weight:700; color:var(--text-main);">
                                📅 Año ${currentYear} <span style="margin:0 8px; color:var(--text-muted); font-weight:400;">/</span> 📂 Todos los Sectores <span style="margin:0 8px; color:var(--text-muted); font-weight:400;">/</span> <span style="color:var(--text-muted); font-size:13px;"><span id="pastorVisibleCount">${activeYearReports.length}</span> reportes</span>
                            </div>
                        </div>
                        <div id="pastorCardsContainer">
                            ${activeYearReports.length === 0 ? '<div class="glass animate-reveal" style="padding:48px; border-radius:24px; text-align:center;"><div style="font-size:48px; margin-bottom:16px; opacity:0.5;">📭</div><p style="color:var(--text-main); font-size:18px; font-weight:700;">No hay reportes para este periodo</p><p style="color:var(--text-muted); font-size:14px; margin-top:8px;">Intenta cambiar los filtros superiores o cargar más historial.</p></div>' : activeYearReports.map(r => renderCard(r)).join('')}
                        </div>
                        ${hasMore ? `
                            <div style="text-align: center; margin-top: 20px; margin-bottom: 40px;">
                                <button id="btnLoadMoreHistory" class="btn-role-switcher" style="padding: 14px 32px; border-radius: 20px; font-weight: 700; color: var(--primary); background: white; border: 1px solid var(--border); box-shadow: var(--shadow-sm); transition: all 0.3s; width:auto; display:inline-flex;">
                                    <span>Ver más reportes anteriores</span>
                                    <span>↓</span>
                                </button>
                            </div>
                        ` : `
                            ${activeYearReports.length > 0 ? `<div style="text-align: center; padding: 32px; color: var(--text-muted); font-size: 13px; font-weight: 600;">Ya no hay más reportes en el historial activo.</div>` : ''}
                        `}
                    </div>
                `;

                innerContentHtml = `
                    ${tabsHtml}
                    <div style="display: flex; gap: 24px; align-items: flex-start; flex-wrap: wrap;">
                        <div class="pastor-tree-container" style="flex: 0 0 300px; max-width: 100%;">
                            ${treeHtml}
                        </div>
                        <div style="flex: 1; min-width: 320px;">
                            ${rightPanelHtml}
                        </div>
                    </div>
                `;
            } else {
                const listHtml = reports.length === 0 ? '<div class="glass" style="padding:48px; border-radius:24px; text-align:center;"><p style="color:var(--text-muted); font-size:16px;">✨ No se encontraron reportes con estos criterios.</p></div>' : reports.map(r => renderCard(r)).join('');

                innerContentHtml = `
                    <div style="margin-top:12px;">${listHtml}</div>
                    ${hasMore ? `
                        <div style="text-align: center; margin-top: 32px; margin-bottom: 40px;">
                            <button id="btnLoadMoreHistory" class="btn-role-switcher" style="padding: 14px 32px; border-radius: 20px; font-weight: 700; color: var(--primary); background: white; border: 1px solid var(--border); box-shadow: var(--shadow-sm); transition: all 0.3s;">
                                <span>Ver más reportes anteriores</span>
                                <span>↓</span>
                            </button>
                        </div>
                    ` : `
                        ${reports.length > 0 ? `<div style="text-align: center; padding: 32px; color: var(--text-muted); font-size: 13px; font-weight: 600;">Ya no hay más reportes en el historial.</div>` : ''}
                    `}
                `;
            }

            return `
                <div class="view-container">
                    <header class="header">
                        <div class="header-user">
                            <p style="font-weight:700; text-transform:uppercase; font-size:10px; letter-spacing:0.15em; margin-bottom:4px; opacity: 0.8;">Auditoría de Registros</p>
                            <h2 style="font-size:28px;">📋 Historial</h2>
                        </div>
                    </header>
                    <div class="dashboard-content" style="padding-bottom: 140px;">
                        ${filterHtml}
                        ${innerContentHtml}
                    </div>
                </div>
            `;
        },

        reporteSector(data) {
            const { sector, coordinator, week, discipliners, totals, percentages, followUp, filters, sectors, weeks } = data;
            const isPastor = App.state.user.role === 'pastor';

            const sectorOptions = sectors.map(s => `<option value="${s.id}" ${filters.sector_id === s.id ? 'selected' : ''}>${s.name}</option>`).join('');
            const weekOptions = weeks.map(w => `<option value="${w.dateStr}" ${filters.report_date === w.dateStr ? 'selected' : ''}>Semana: ${App.formatFriendlyDate(w.saturday)}</option>`).join('');

            return `
                <div class="view-container">
                    <header class="header">
                        <div class="header-user">
                            <p style="font-weight:700; text-transform:uppercase; font-size:10px; letter-spacing:0.15em; margin-bottom:4px; opacity: 0.8;">Reporte por Sector</p>
                            <h2 style="font-size:24px;">${sector.name}</h2>
                        </div>
                        <a href="#/dashboard" class="btn-back-modern">
                            <span>←</span> Volver
                        </a>
                    </header>

                    <div class="dashboard-content" style="padding-bottom: 100px;">
                        <!-- Filtros -->
                        <div class="glass animate-reveal" style="margin-bottom:24px; padding:20px; border-radius:24px;">
                            <div style="display:grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap:16px;">
                                ${isPastor ? `
                                <div class="filter-group">
                                    <label style="font-size:11px; font-weight:800; color:var(--text-muted);">SECTOR</label>
                                    <select id="rsSector" class="form-control-premium">${sectorOptions}</select>
                                </div>` : ''}
                                <div class="filter-group">
                                    <label style="font-size:11px; font-weight:800; color:var(--text-muted);">SEMANA</label>
                                    <select id="rsWeek" class="form-control-premium">${weekOptions}</select>
                                </div>
                                <div class="filter-group" style="display:flex; align-items:flex-end;">
                                    <button id="btnDownloadSectorReport" class="btn btn-primary" style="height:46px; border-radius:14px; font-size:13px;">
                                        📥 Descargar PDF
                                    </button>
                                </div>
                            </div>
                        </div>

                        <!-- Encabezado del Reporte -->
                        <div class="glass animate-reveal" style="margin-bottom:24px; padding:24px; border-radius:24px; background: linear-gradient(135deg, white 0%, #f8fafc 100%);">
                            <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:20px;">
                                <div>
                                    <div style="font-size:11px; font-weight:800; color:var(--text-muted); text-transform:uppercase;">Coordinador Responsable</div>
                                    <div style="font-size:18px; font-weight:800; color:var(--primary);">${coordinator.name || 'Sin asignar'}</div>
                                </div>
                                <div style="text-align:right;">
                                    <div style="font-size:11px; font-weight:800; color:var(--text-muted); text-transform:uppercase;">Semana del Reporte</div>
                                    <div style="font-size:16px; font-weight:700; color:var(--accent);">${App.formatFriendlyDate(new Date(week.dateStr + 'T12:00:00'))}</div>
                                </div>
                            </div>
                        </div>

                        <!-- Tabla Principal -->
                        <div class="glass animate-reveal" style="margin-bottom:24px; border-radius:24px; overflow:hidden;">
                            <div style="overflow-x:auto;">
                                <table style="width:100%; border-collapse:collapse;">
                                    <thead>
                                        <tr style="background:#f1f5f9;">
                                            <th style="padding:16px; text-align:left; font-size:11px; font-weight:800; color:var(--text-muted); border-bottom:2px solid #e2e8f0;">DISCIPULADOR</th>
                                            <th style="padding:16px; text-align:center; font-size:11px; font-weight:800; color:var(--text-muted); border-bottom:2px solid #e2e8f0;">VIE ASIST.</th>
                                            <th style="padding:16px; text-align:center; font-size:11px; font-weight:800; color:var(--text-muted); border-bottom:2px solid #e2e8f0;">VIE INASIST.</th>
                                            <th style="padding:16px; text-align:center; font-size:11px; font-weight:800; color:var(--text-muted); border-bottom:2px solid #e2e8f0;">SÁB ASIST.</th>
                                            <th style="padding:16px; text-align:center; font-size:11px; font-weight:800; color:var(--text-muted); border-bottom:2px solid #e2e8f0;">SÁB INASIST.</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        ${discipliners.map(d => `
                                            <tr style="border-bottom:1px solid #f1f5f9;">
                                                <td style="padding:16px; font-weight:700; color:var(--text-main);">${d.name}</td>
                                                <td style="padding:16px; text-align:center; font-weight:700; color:var(--success);">${d.friAtt}</td>
                                                <td style="padding:16px; text-align:center; font-weight:700; color:var(--danger);">${d.friAbs}</td>
                                                <td style="padding:16px; text-align:center; font-weight:700; color:var(--success);">${d.satAtt}</td>
                                                <td style="padding:16px; text-align:center; font-weight:700; color:var(--danger);">${d.satAbs}</td>
                                            </tr>
                                        `).join('')}
                                        <tr style="background:#f8fafc; font-weight:900;">
                                            <td style="padding:16px; color:var(--primary);">TOTALES</td>
                                            <td style="padding:16px; text-align:center; color:var(--success);">${totals.friAtt}</td>
                                            <td style="padding:16px; text-align:center; color:var(--danger);">${totals.friAbs}</td>
                                            <td style="padding:16px; text-align:center; color:var(--success);">${totals.satAtt}</td>
                                            <td style="padding:16px; text-align:center; color:var(--danger);">${totals.satAbs}</td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                        </div>

                        <!-- Bloque de Porcentajes -->
                        <div class="metrics-grid" style="grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); margin-bottom: 24px;">
                            <div class="metric-card" style="padding:16px; text-align:center;">
                                <div style="font-size:24px; color:var(--success); font-weight:900;">${percentages.friAtt}%</div>
                                <p style="font-size:10px;">% ASIST. VIERNES</p>
                            </div>
                            <div class="metric-card" style="padding:16px; text-align:center;">
                                <div style="font-size:24px; color:var(--danger); font-weight:900;">${percentages.friAbs}%</div>
                                <p style="font-size:10px;">% INASIST. VIERNES</p>
                            </div>
                            <div class="metric-card" style="padding:16px; text-align:center;">
                                <div style="font-size:24px; color:var(--success); font-weight:900;">${percentages.satAtt}%</div>
                                <p style="font-size:10px;">% ASIST. SÁBADO</p>
                            </div>
                            <div class="metric-card" style="padding:16px; text-align:center;">
                                <div style="font-size:24px; color:var(--danger); font-weight:900;">${percentages.satAbs}%</div>
                                <p style="font-size:10px;">% INASIST. SÁBADO</p>
                            </div>
                        </div>

                        <!-- Lista de Seguimiento -->
                        <div class="glass animate-reveal" style="padding:24px; border-radius:24px; border:1px solid #fee2e2; background: linear-gradient(135deg, white 0%, #fff5f5 100%);">
                            <h4 style="margin-bottom:16px; color:var(--danger);">⚠️ Lista de Seguimiento Automática</h4>
                            <p style="font-size:12px; color:var(--text-muted); margin-bottom:16px;">Hermanos con <strong>5 o más faltas</strong> en el módulo actual.</p>
                            
                            ${followUp.length === 0 ? `
                                <div style="padding:20px; text-align:center; color:var(--text-muted); font-style:italic; font-size:14px;">No hay personas en seguimiento actualmente.</div>
                            ` : `
                                <div style="overflow-x:auto;">
                                    <table style="width:100%; border-collapse:collapse;">
                                        <thead>
                                            <tr style="border-bottom:2px solid #fee2e2;">
                                                <th style="padding:12px; text-align:left; font-size:11px; color:var(--text-muted);">NOMBRE</th>
                                                <th style="padding:12px; text-align:left; font-size:11px; color:var(--text-muted);">DISCIPULADOR</th>
                                                <th style="padding:12px; text-align:center; font-size:11px; color:var(--text-muted);">FALTAS</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            ${followUp.map(f => `
                                                <tr style="border-bottom:1px solid rgba(239, 68, 68, 0.1);">
                                                    <td style="padding:12px; font-weight:700; color:var(--text-main);">${f.name}</td>
                                                    <td style="padding:12px; font-size:13px; color:var(--text-muted);">${f.discipliner}</td>
                                                    <td style="padding:12px; text-align:center;"><span class="pill pill-danger" style="padding:4px 10px; border-radius:12px;">${f.absences}</span></td>
                                                </tr>
                                            `).join('')}
                                        </tbody>
                                    </table>
                                </div>
                            `}
                        </div>
                    </div>
                </div>
            `;
        },

        estadisticas(stats, meta, filters) {
            const user = App.state.user;
            const activeMode = App.state.activeMode || user.role;
            const isPastor = user.role === 'pastor';
            const isDiscipulador = activeMode === 'discipulador';

            let filterHtml = '';
            // Si es Pastor o niveles administrativos superiores, mostramos toda la jerarquía
            if (isPastor || user.role === 'pastorexterno' || user.role === 'coordinador') {
                filterHtml = `
                    <div class="glass animate-reveal" style="margin-bottom:28px; padding:24px; border-radius:32px; border:1px solid var(--glass-border);">
                        <h3 class="section-title" style="margin-bottom:16px; font-size:14px; letter-spacing:0.05em; color:var(--text-muted);">Jerarquía de Análisis</h3>
                        <div class="stats-filter-grid">
                            <div class="filter-group">
                                <label style="font-size:10px; font-weight:800; color:var(--text-muted); text-transform:uppercase; margin-bottom:6px; display:block;">Discipulado General</label>
                                <select id="stSector" class="form-control-premium">
                                    <option value="">Todos los Sectores</option>
                                    ${meta.sectors.map(s => `<option value="${s.id}" ${filters.sector_id === s.id ? 'selected' : ''}>${s.name}</option>`).join('')}
                                </select>
                            </div>
                            <div class="filter-group">
                                <label style="font-size:10px; font-weight:800; color:var(--text-muted); text-transform:uppercase; margin-bottom:6px; display:block;">Discipulador</label>
                                <select id="stLeader" class="form-control-premium">
                                    <option value="">Cualquier Discipulador</option>
                                    ${meta.discipliners.filter(l => App.isReporter(l, meta)).map(l => `<option value="${l.id}" ${filters.reporter_id === l.id ? 'selected' : ''}>${l.name}</option>`).join('')}
                                </select>
                            </div>
                            <div class="filter-group">
                                <label style="font-size:10px; font-weight:800; color:var(--text-muted); text-transform:uppercase; margin-bottom:6px; display:block;">Oveja</label>
                                <select id="stSheep" class="form-control-premium">
                                    <option value="">Todas las Personas</option>
                                    ${meta.sheep.map(p => `<option value="${p.id}" ${filters.person_id === p.id ? 'selected' : ''}>${p.name}</option>`).join('')}
                                </select>
                            </div>
                        </div>
                    </div>`;
            } else if (isDiscipulador) {
                // Si es discipulador, solo permitimos filtrar por sus ovejas
                filterHtml = `
                    <div class="glass animate-reveal" style="margin-bottom:28px; padding:24px; border-radius:32px; border:1px solid var(--glass-border);">
                        <h3 class="section-title" style="margin-bottom:16px; font-size:14px; letter-spacing:0.05em; color:var(--text-muted);">Filtrar por Integrante</h3>
                        <div class="stats-filter-grid">
                            <div class="filter-group">
                                <label style="font-size:10px; font-weight:800; color:var(--text-muted); text-transform:uppercase; margin-bottom:6px; display:block;">Seleccionar Persona</label>
                                <select id="stSheep" class="form-control-premium">
                                    <option value="">Todo mi discipulado</option>
                                    ${meta.sheep.map(p => `<option value="${p.id}" ${filters.person_id === p.id ? 'selected' : ''}>${p.name}</option>`).join('')}
                                </select>
                            </div>
                        </div>
                    </div>`;
            }

            return `
                <div class="view-container">
                    <header class="header">
                        <div class="header-user">
                            <p style="font-weight:700; text-transform:uppercase; font-size:10px; letter-spacing:0.15em; margin-bottom:4px; opacity: 0.8;">Inteligencia de Datos</p>
                            <h2 style="font-size:28px;">📊 Estadísticas</h2>
                        </div>
                    </header>
                    
                    <div class="dashboard-content">
                        ${filterHtml}
                        
                        <div style="display:flex; justify-content:flex-end; margin-bottom:16px;">
                            <button id="btnDownloadStats" class="btn-role-switcher" style="background:var(--primary); color:white; border:none; box-shadow:0 8px 16px var(--primary)30;">
                                <span class="icon-badge" style="background:rgba(255,255,255,0.2); color:white;">📥</span>
                                <span>Descargar estadísticas</span>
                            </button>
                        </div>

                        <div id="statsPrintArea">
                            <div class="stats-summary-grid-enterprise">
                                <div class="stat-card-enterprise main animate-reveal delay-1" style="border-left: 6px solid var(--accent); grid-column: 1 / -1;">
                                    <div style="display:flex; justify-content:space-between; align-items:flex-start; width:100%;">
                                        <div>
                                            <div id="statAttendanceRate" class="stat-main-val" style="font-size:32px;">${stats.attendanceRate}%</div>
                                            <div class="stat-label-sub">Tasa General (Semanal)</div>
                                            <div style="font-size:11px; color:var(--text-muted); margin-top:6px; font-weight:600;"><span style="color:#10B981;">Asistencias: <span id="statTotalAttended">${stats.totalAttended}</span></span> | <span style="color:#EF4444;">Faltas: <span id="statTotalAbsent">${stats.totalAbsent}</span></span></div>
                                        </div>
                                        <div style="text-align:right;">
                                            <div id="statReportsCount" class="stat-val-sm" style="font-size:20px; color:var(--primary);">${stats.totalReports}</div>
                                            <div class="stat-label-sub">Reportes Analizados</div>
                                        </div>
                                    </div>
                                    <div class="stat-progress-bg" style="margin-top:12px;"><div id="statAttendanceProgress" class="stat-progress-fill" style="width:${stats.attendanceRate}%;"></div></div>
                                </div>
                                <div class="stat-card-enterprise animate-reveal delay-2" style="border-left: 6px solid #10B981; background:rgba(16,185,129,0.02);">
                                    <h4 style="margin:0 0 12px 0; font-size:12px; font-weight:800; color:#10B981; text-transform:uppercase; letter-spacing:1px;">Viernes</h4>
                                    <div style="display:grid; grid-template-columns: 1fr 1fr; gap:16px; width:100%;">
                                        <div style="text-align:center;">
                                            <div id="statFriAttended" style="font-size:28px; font-weight:900; color:#10B981; line-height:1.2;">${stats.friAttended}</div>
                                            <div style="font-size:10px; font-weight:700; color:var(--text-muted); text-transform:uppercase; margin-top:4px;">Asistencias</div>
                                        </div>
                                        <div style="text-align:center;">
                                            <div id="statFriAbsent" style="font-size:28px; font-weight:900; color:#EF4444; line-height:1.2;">${stats.friAbsent}</div>
                                            <div style="font-size:10px; font-weight:700; color:var(--text-muted); text-transform:uppercase; margin-top:4px;">Ausencias</div>
                                        </div>
                                    </div>
                                </div>
                                <div class="stat-card-enterprise animate-reveal delay-3" style="border-left: 6px solid #10B981; background:rgba(16,185,129,0.02);">
                                    <h4 style="margin:0 0 12px 0; font-size:12px; font-weight:800; color:#10B981; text-transform:uppercase; letter-spacing:1px;">Sábado</h4>
                                    <div style="display:grid; grid-template-columns: 1fr 1fr; gap:16px; width:100%;">
                                        <div style="text-align:center;">
                                            <div id="statSatAttended" style="font-size:28px; font-weight:900; color:#10B981; line-height:1.2;">${stats.satAttended}</div>
                                            <div style="font-size:10px; font-weight:700; color:var(--text-muted); text-transform:uppercase; margin-top:4px;">Asistencias</div>
                                        </div>
                                        <div style="text-align:center;">
                                            <div id="statSatAbsent" style="font-size:28px; font-weight:900; color:#EF4444; line-height:1.2;">${stats.satAbsent}</div>
                                            <div style="font-size:10px; font-weight:700; color:var(--text-muted); text-transform:uppercase; margin-top:4px;">Ausencias</div>
                                        </div>
                                    </div>
                                </div>
                            </div>

                            <div style="display:grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap:24px; margin-top:24px;">
                                <div class="chart-container-premium glass animate-reveal delay-4" style="background: white;">
                                    <div style="margin-bottom:20px;">
                                        <h4 style="margin:0; font-weight:800; color:var(--primary); font-size:15px;">Comparativa Semanal</h4>
                                        <p style="margin:4px 0 0 0; font-size:11px; color:var(--text-muted);">Porcentaje Viernes vs Sábado</p>
                                    </div>
                                    <div style="height: 280px; position: relative;">
                                        <canvas id="detailsChart"></canvas>
                                    </div>
                                </div>

                                <div class="chart-container-premium glass animate-reveal delay-4" style="background: white; animation-delay: 0.5s;">
                                    <div style="margin-bottom:20px;">
                                        <h4 style="margin:0; font-weight:800; color:var(--primary); font-size:15px;">Tendencia de Crecimiento</h4>
                                        <p style="margin:4px 0 0 0; font-size:11px; color:var(--text-muted);">Evolución del promedio general</p>
                                    </div>
                                    <div style="height: 280px; position: relative;">
                                        <canvas id="averageChart"></canvas>
                                    </div>
                                </div>
                            </div>

                            <div style="display:flex; align-items:center; gap:12px; margin-bottom:24px; margin-top:24px;" class="animate-reveal" style="animation-delay: 0.6s;">
                                <div style="width:4px; height:24px; background:#2563eb; border-radius:4px;"></div>
                                <h4 style="margin:0; font-weight:800; color:var(--primary); font-size:20px;">Tabla de Registro Semanal</h4>
                            </div>
                            <div class="glass animate-reveal" style="padding:24px; border-radius:32px; border:1px solid rgba(0,0,0,0.03); background: white; animation-delay: 0.7s;">
                                <div style="overflow-x: auto; margin: -10px;">
                                    <table class="attendance-matrix-table" style="width:100%; border-collapse: separate; border-spacing: 0 12px; min-width: 600px;">
                                        <thead>
                                            <tr>
                                                <th class="sticky-column-header" style="text-align:left; padding:0 14px; font-size:10px; color:var(--text-muted); text-transform:uppercase; font-weight:800; background: white; position: sticky; left: 0; z-index: 20; box-shadow: 4px 0 8px rgba(0,0,0,0.02); min-width: 110px;">Persona</th>
                                                ${stats.matrix.dates.map(d => {
                const { friday, saturday } = App.getReportWeekRange(d);
                const month = saturday.toLocaleDateString('es-ES', { month: 'short' });
                return `
                                                    <th style="padding:15px 10px; background: #f8fafc; border-bottom: 2px solid #edf2f7; min-width:110px;">
                                                        <div style="font-size:10px; color:var(--primary); font-weight:900; text-transform:uppercase; margin-bottom:8px;">${friday.getDate()}-${saturday.getDate()} ${month}</div>
                                                        <div style="display:flex; justify-content:center; gap:16px; font-size:8px; color:var(--text-muted); font-weight:800; letter-spacing:1px; opacity:0.6;">
                                                            <span style="width:30px;">VIE</span>
                                                            <span style="width:30px;">SÁB</span>
                                                        </div>
                                                    </th>`;
            }).join('')}
                                            </tr>
                                        </thead>
                                        <tbody id="attendanceMatrixBody">
                                            ${stats.matrix.rows.map(row => App.renderMatrixRow(row, stats.matrix.dates)).join('')}
                                        </tbody>
                                    </table>
                                </div>
                            </div>

                            <div class="glass" style="margin-top:24px; padding:28px; border-radius:32px; border:1px solid rgba(0,0,0,0.03); background: white;">
                                <h4 style="margin-bottom:20px; font-weight:800; color:var(--primary);">Resumen de Cobertura Pastoral</h4>
                                <div class="data-table-container">
                                    <div class="data-row">
                                        <div class="data-info">
                                            <div class="action-card-icon sm" style="background:rgba(79,106,254,0.1); color:var(--primary-alt); width:36px; height:36px; font-size:14px;">👥</div>
                                            <div class="data-title">Total Miembros Registrados</div>
                                        </div>
                                        <div style="text-align:right; font-weight:800; color:var(--primary);">${meta.sheep.length}</div>
                                    </div>
                                    <div class="data-row">
                                        <div class="data-info">
                                            <div class="action-card-icon sm" style="background:rgba(245,158,11,0.1); color:#F59E0B; width:36px; height:36px; font-size:14px;">📉</div>
                                            <div class="data-title">Ovejas Esperadas (Filtro Actual)</div>
                                        </div>
                                        <div style="text-align:right; font-weight:800; color:var(--primary);">${stats.totalExpected}</div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        },

        admin(sectors, profiles, people, sectorMap, unassignedSheep) {
            const user = App.state.user;
            const activeMode = App.state.activeMode || user.role;
            const canManage = user.role === 'pastor' || activeMode === 'coordinador';

            const sectorColors = [
                { border: '#3B82F6', badge: '#3B82F6', coordBg: '#EFF6FF' },
                { border: '#8B5CF6', badge: '#8B5CF6', coordBg: '#F5F3FF' },
                { border: '#10B981', badge: '#10B981', coordBg: '#ECFDF5' }
            ];

            // 1. ESTRUCTURA DE SECTORES (Diseño de la Imagen)
            let hierarchyHtml = sectors.map((s, idx) => {
                const entry = sectorMap[s.id];
                const sectorIdx = idx % 3;
                const colors = [
                    { badge: '#2563EB', coordBg: '#EFF6FF', badge20: '#2563EB33' },
                    { badge: '#7C3AED', coordBg: '#F5F3FF', badge20: '#7C3AED33' },
                    { badge: '#059669', coordBg: '#ECFDF5', badge20: '#05966933' }
                ][sectorIdx];

                const renderMemberCard = (profile, isContextCoord) => {
                    // 1. Corrección de datos y roles
                    let rawRoles = [profile.role, ...(profile.additional_roles || [])];

                    // Corrección específica para Fernando Castro solicitada por el usuario
                    if (profile.name.includes("Fernando Castro")) {
                        rawRoles = ["discipulador"];
                    }

                    // 2. Deduplicar y Capitalizar (Primera letra Mayúscula)
                    const uniqueRoles = [...new Set(rawRoles.map(r => r.toLowerCase()))];
                    const roleBadges = uniqueRoles.map(r => {
                        const capitalized = r.charAt(0).toUpperCase() + r.slice(1);
                        return `<span class="sector-role-tag">${capitalized}</span>`;
                    }).join('');

                    return `
                        <div class="sector-member ${isContextCoord ? 'sector-coordinator' : ''}" style="${isContextCoord ? `--badge-color: ${colors.badge}; --coord-bg: ${colors.coordBg}; --badge-color-20: ${colors.badge20}` : `--badge-color: ${colors.badge}`}">
                            <div class="sector-avatar ${!isContextCoord ? 'sm' : ''}" style="background: ${colors.badge}">${profile.name.charAt(0)}</div>
                            <div class="member-info">
                                <div class="member-name">${profile.name}</div>
                                <div class="member-roles">${roleBadges}</div>
                                ${isContextCoord ? '<div class="sector-coord-badge">COORDINADOR DE SECTOR</div>' : ''}
                            </div>
                        </div>`;
                };

                const discListHtml = entry.disciples.length === 0 ? '<p class="sector-empty">Sin discipuladores</p>' : entry.disciples.map(d => renderMemberCard(d.profile, false)).join('');

                return `
                    <div class="sector-column" style="--badge-color: ${colors.badge}">
                        <div class="sector-column-header">
                            <span class="sector-icon">🏠</span> 
                            <span style="color: ${colors.badge}">Sector ${idx + 1}</span>
                        </div>
                        <div class="sector-members-label">Coordinador</div>
                        ${entry.coordinator ? renderMemberCard(entry.coordinator, true) : '<div class="sector-empty">Sin asignar</div>'}
                        <div class="sector-members-label" style="margin-top:20px;">Discipuladores</div>
                        ${discListHtml}
                    </div>`;
            }).join('');

            // 2. NUEVA SECCIÓN: GESTIÓN DE OVEJAS (Separada y Pulida)
            // 3. SECCION: PENDIENTES DE ASIGNAR (Crucial para corregir el error de personas invisibles)
            let unassignedListHtml = unassignedSheep.length === 0
                ? '<div class="empty-state-card"><p>✨ No hay personas sin discipulador asignado</p></div>'
                : `<div class="unassigned-container">
                    ${unassignedSheep.map(sh => `
                        <div class="sheep-item-minimal unassigned">
                            <span class="sheep-name">👤 ${sh.name}</span>
                            <div class="sheep-actions-minimal">
                                <button class="btn-tool reassign-btn highlight" data-id="${sh.id}" data-name="${sh.name}" title="Asignar Discipulador">➕ Asignar</button>
                                <button class="btn-tool delete-sheep-btn danger" data-id="${sh.id}" data-name="${sh.name}" title="Eliminar Registro">🗑️ Eliminar</button>
                            </div>
                        </div>`).join('')}
                   </div>`;

            let sheepManagementHtml = sectors.map((s, idx) => {
                const entry = sectorMap[s.id];
                const colors = sectorColors[idx % sectorColors.length];

                const renderModernGroup = (leader, sheep, roleLabel, isCoord) => {
                    const rowHtml = sheep.length === 0
                        ? `<div class="sheep-empty-state">No hay integrantes asignados</div>`
                        : sheep.map(sh => `
                            <div class="sheep-row-modern">
                                <div class="sheep-info-minimal">
                                    <span class="sheep-emoji">👤</span>
                                    <span class="sheep-name-text">${sh.name}</span>
                                </div>
                                <div class="sheep-actions-modern">
                                    <button class="btn-tool-modern reassign-btn" data-id="${sh.id}" data-name="${sh.name}" title="Reasignar">🔄</button>
                                    <button class="btn-tool-modern unassign-btn danger" data-id="${sh.id}" data-name="${sh.name}" title="Desasignar">✕</button>
                                </div>
                            </div>
                        `).join('');

                    return `
                        <div class="discipleship-card ${isCoord ? 'is-coordinator' : ''}">
                            <div class="discipleship-header">
                                <div class="leader-info">
                                    <div class="leader-avatar" style="background:${colors.badge};">${leader.name.charAt(0)}</div>
                                    <div>
                                        <div class="leader-name">${leader.name}</div>
                                        <div class="leader-role-badge">${roleLabel}</div>
                                    </div>
                                </div>
                            </div>
                            <div class="sheep-list-modern">
                                ${rowHtml}
                            </div>
                        </div>`;
                };

                let groups = [];
                if (entry.coordinator) {
                    groups.push(renderModernGroup(entry.coordinator, entry.coordSheep, 'Coordinador', true));
                }
                entry.disciples.forEach(d => {
                    groups.push(renderModernGroup(d.profile, d.sheep, 'Discipulador', false));
                });

                return `
                    <div class="sector-column-premium">
                        <div class="sector-header-premium" style="--sector-color: ${colors.badge};">
                            <div class="sector-icon-box" style="background: ${colors.badge}; font-size: 14px;">📍</div>
                            <div class="sector-title-wrapper">
                                <span class="sector-label">DISTRITO / AREA</span>
                                <h3 class="sector-name">${s.name}</h3>
                            </div>
                            <div class="sector-count">${entry.coordSheep.length + entry.disciples.reduce((acc, d) => acc + d.sheep.length, 0)}</div>
                        </div>
                        <div class="sector-body-modern">
                            ${groups.join('') || '<div class="empty-notif">No hay grupos registrados</div>'}
                        </div>
                    </div>`;
            }).join('');

            // Opciones para formularios agrupadas por sector
            let personAssignmentOptions = '';
            const groupedBySector = {};

            profiles.forEach(p => {
                const isLeader = [p.role, ...(p.additional_roles || [])].some(r => ['discipulador', 'coordinador', 'pastor', 'pastorexterno'].includes(r));
                if (isLeader) {
                    const sid = p.sector_id || 'global';
                    const sname = p.sector ? p.sector.name : 'Área Global / Sin Sector';
                    if (!groupedBySector[sid]) groupedBySector[sid] = { name: sname, users: [] };
                    groupedBySector[sid].users.push(p);
                }
            });

            Object.values(groupedBySector).forEach(g => {
                personAssignmentOptions += `<optgroup label="📍 SECTOR: ${g.name.toUpperCase()}">`;
                g.users.forEach(u => {
                    personAssignmentOptions += `<option value="${u.id}">${u.name} — (${u.role})</option>`;
                });
                personAssignmentOptions += `</optgroup>`;
            });

            let sectorSelectOptions = sectors.map(s => `<option value="${s.id}">${s.name}</option>`).join('');

            // Mapa de colores sutiles para sombreado de filas por sector
            const sectorShades = {};
            sectors.forEach((s, idx) => {
                const colors = ['#F1F5F9', '#E0E7FF', '#EDE9FE', '#DCFCE7', '#FEF3C7'];
                sectorShades[s.id] = colors[idx % colors.length];
            });

            // Ordenar perfiles por Sector y luego por Nombre
            const sortedProfiles = [...profiles].sort((a, b) => {
                const sA = a.sector ? a.sector.name : 'ZZZ'; // Mandar sin sector al final
                const sB = b.sector ? b.sector.name : 'ZZZ';
                if (sA !== sB) return sA.localeCompare(sB);
                return a.name.localeCompare(b.name);
            });

            let userRows = sortedProfiles.map(p => {
                const sName = p.sector ? (p.sector.name || 'Sin nombre') : '<span style="color:var(--text-muted);">Sin asignar</span>';
                const rowStyle = p.sector_id ? `background-color: ${sectorShades[p.sector_id]};` : '';
                const roleFormatted = p.role.charAt(0).toUpperCase() + p.role.slice(1);

                return `
                    <tr style="${rowStyle} border-bottom: 1px solid rgba(15, 23, 42, 0.08);">
                        <td style="padding: 14px 20px;"><code style="font-size:11px; color:var(--text-muted);">${p.id.substring(0, 8)}</code></td>
                        <td style="padding: 14px 20px; font-weight:700; color:var(--primary);">${p.name}</td>
                        <td style="padding: 14px 20px;">
                            <span class="role-badge-pill" data-role="${p.role}">${roleFormatted}</span>
                        </td>
                        <td style="padding: 14px 20px; font-weight:600; font-size:13px; color:var(--text-main);">${sName}</td>
                    </tr>`;
            }).join('');

            return `
                <div class="view-container">
                    <div class="header"><div class="header-user"><h2>⚙️ Panel de Administración</h2><p>Pastorado General</p></div></div>
                    <div class="dashboard-content">

                        <!-- NUEVO: Acceso a Reporte por Sectores -->
                        <div class="admin-section" style="background: linear-gradient(135deg, #4f46e50a 0%, #7c3aed12 100%); border: 1px solid #7c3aed33;">
                            <div style="display:flex; justify-content:space-between; align-items:center;">
                                <div>
                                    <h3 class="section-title" style="color: #4f46e5; margin-bottom: 4px;">📊 Reporte por Sectores</h3>
                                    <p style="font-size:12px; color:var(--text-muted);">Visualiza el resumen semanal de asistencia de todos los sectores.</p>
                                </div>
                                <a href="#/reporte-sector" class="btn btn-primary" style="width:auto; padding: 10px 20px; font-size:14px; border-radius:12px;">Abrir Reportes</a>
                            </div>
                        </div>
                        
                        <!-- 1. Estructura de Sectores (Image Match) -->
                        <div class="admin-section">
                            <h3 class="section-title">📊 Estructura por Sectores</h3>
                            <div class="sector-grid">${hierarchyHtml}</div>
                        </div>

                        <!-- 2. Personas sin Discipulador (Fix Visual) -->
                        <div class="admin-section highlight-section">
                            <h3 class="section-title">👤 Personas Pendientes de Asignación</h3>
                            <p style="font-size:12px; color:var(--text-muted); margin-bottom:16px;">Estas personas no tienen un discipulador asignado actualmente.</p>
                            ${unassignedListHtml}
                        </div>

                        <!-- 3. Gestión de Ovejas -->
                        <div class="admin-section">
                            <h3 class="section-title">🏘️ Organización por Sectores</h3>
                            <div class="admin-main-grid-modern">${sheepManagementHtml}</div>
                        </div>

                        <!-- 4. Formularios de Acción High-Fidelity -->
                        <div class="admin-section">
                            <div class="section-header-modern">
                                <div class="section-icon-premium">🛠️</div>
                                <div>
                                    <h3 class="section-title-modern">Herramientas de Gestión</h3>
                                    <p class="section-subtitle-modern">Acciones rápidas para el control de la estructura</p>
                                </div>
                            </div>
                            
                            <div class="admin-tool-grid">
                                <div class="admin-tool-card glass-premium">
                                    <div class="tool-card-head">
                                        <div class="tool-badge">NUEVO</div>
                                        <h4>Asignar Nueva Persona</h4>
                                    </div>
                                    <form id="personForm" class="admin-tool-form">
                                        <div class="field-group-modern">
                                            <label><span class="label-icon">👤</span> Nombre Completo</label>
                                            <input type="text" id="personName" class="input-premium" placeholder="Ej. Juan Pérez" required>
                                        </div>
                                        <div class="field-group-modern">
                                            <label><span class="label-icon">🤝</span> Discipulador Responsable</label>
                                            <select id="assignToUser" class="select-premium" required>
                                                <option value="" disabled selected>-- Seleccionar responsable --</option>
                                                ${personAssignmentOptions}
                                            </select>
                                        </div>
                                        <button type="submit" id="btnSavePerson" class="btn-premium-action">
                                            <span class="btn-text">Crear y Asignar</span>
                                            <span class="btn-arrow">→</span>
                                        </button>
                                    </form>
                                </div>

                                <div class="admin-tool-card glass-premium">
                                    <div class="tool-card-head">
                                        <div class="tool-badge info">MOVIMIENTO</div>
                                        <h4>Reubicar Discipulador</h4>
                                    </div>
                                    <form id="userSectorForm" class="admin-tool-form">
                                        <div class="field-group-modern">
                                            <label><span class="label-icon">👔</span> Discipulador</label>
                                            <select id="usUserSelect" class="select-premium" required>
                                                <option value="" disabled selected>-- Seleccionar discipulador --</option>
                                                ${personAssignmentOptions}
                                            </select>
                                        </div>
                                        <div class="field-group-modern">
                                            <label><span class="label-icon">🏘️</span> Nuevo Sector</label>
                                            <select id="usSectorSelect" class="select-premium" required>
                                                <option value="" disabled selected>-- Seleccionar sector --</option>
                                                ${sectorSelectOptions}
                                            </select>
                                        </div>
                                        <button type="submit" id="btnSaveUserSector" class="btn-premium-action secondary">
                                            <span class="btn-text">Confirmar Reubicación</span>
                                            <span class="btn-arrow">→</span>
                                        </button>
                                    </form>
                                </div>
                            </div>
                        </div>

                         <div class="admin-section">
                            <h3 class="section-title">👥 Directorio de Discipuladores</h3>
                            <div class="table-responsive">
                                <table class="data-table">
                                    <thead><tr><th>ID</th><th>Nombre</th><th>Rol Principal</th><th>Sector</th></tr></thead>
                                    <tbody>${userRows || '<tr><td colspan="4">No hay datos</td></tr>'}</tbody>
                                </table>
                            </div>
                        </div>

                        <!-- 5. CONFIGURACIÓN DE SEMANAS LIBRES (NUEVO) -->
                        <div class="admin-section glass-premium" style="margin-top:40px; border:2px solid var(--accent-glow);">
                            <div class="section-header-modern">
                                <div class="section-icon-premium" style="background:rgba(16,185,129,0.1); color:#10B981;">📅</div>
                                <div>
                                    <h3 class="section-title-modern">Configuración de Semanas Libres</h3>
                                    <p class="section-subtitle-modern">Marca las fechas que NO requieren pase de asistencia</p>
                                </div>
                            </div>
                            
                            <div style="background: #f8fafc; padding: 20px; border-radius: 16px; margin-top: 16px;">
                                <div id="freeWeeksGrid" style="display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 12px; max-height: 400px; overflow-y: auto; padding: 4px;">
                                    <!-- Cargado dinámicamente -->
                                    <p style="color:var(--text-muted); font-size:13px;">Generando calendario...</p>
                                </div>
                                <div style="margin-top: 20px; padding-top: 16px; border-top: 1px solid rgba(0,0,0,0.05); text-align: right;">
                                    <button id="btnSaveFreeWeeks" class="btn-premium-action" style="width: auto; padding: 12px 32px;">
                                        Guardar Configuración Global
                                    </button>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        },

        estructura(sectors, profiles, people, sectorMap, unassignedSheep) {
            let gridHtml = sectors.map((s, idx) => {
                const entry = sectorMap[s.id];
                const colors = [
                    { b: '#3B82F6', bg: '#EFF6FF' },
                    { b: '#8B5CF6', bg: '#F5F3FF' },
                    { b: '#10B981', bg: '#ECFDF5' }
                ][idx % 3];

                const discHtml = entry.disciples.map(d => `
                    <div class="struct-disciple-item">
                        <span class="struct-disc-name">${d.profile.name}</span>
                        <span class="struct-disc-count">👥 ${d.sheep.length}</span>
                    </div>`).join('');

                return `
                    <div class="struct-sector-card" style="--sector-color: ${colors.b}; --sector-bg: ${colors.bg}">
                        <div class="struct-sector-header">🏘️ Sector: ${s.name}</div>
                        <div class="struct-leader-box">
                            <div class="struct-label">Coordinador Responsable</div>
                            <div class="struct-leader-name">${entry.coordinator ? entry.coordinator.name : 'No asignado'}</div>
                            <div class="struct-leader-stat">Ovejas directas: ${entry.coordSheep.length}</div>
                        </div>
                        <div class="struct-label" style="margin-top:16px;">Equipo de Discipuladores</div>
                        <div class="struct-disciples-list">
                            ${discHtml || '<p class="struct-empty">Sin equipo asignado</p>'}
                        </div>
                    </div>`;
            }).join('');

            return `
                <div class="view-container">
                    <div class="header">
                        <div class="header-user"><h2>🏘️ Estructura Pastoral</h2><p>Organización por sectores</p></div>
                    </div>
                    <div class="dashboard-content">
                        <div class="struct-grid-modern">${gridHtml}</div>
                    </div>
                </div>
            `;
        }
    },

    components: {
        bottomNav(activeId) {
            const nav = document.createElement('nav');
            nav.className = 'bottom-nav';

            let tabs = [
                { id: 'dashboard', icon: '🏠', label: 'Inicio', link: '#/dashboard' },
                { id: 'reporte', icon: '📝', label: 'Reporte', link: '#/reporte' },
                { id: 'historial', icon: '📋', label: 'Historial', link: '#/historial' },
                { id: 'estadisticas', icon: '📊', label: 'Estadísticas', link: '#/estadisticas' }
            ];

            // Ocultar pestaña de reporte para supervisores que no tienen ovejas asignadas
            if (App.state.user && App.state.cache.metadata) {
                if (!App.isReporter(App.state.user, App.state.cache.metadata)) {
                    tabs = tabs.filter(t => t.id !== 'reporte');
                }
            }

            const brandHtml = `
                <div class="sidebar-brand-desktop" style="padding: 24px 32px; margin-bottom: 24px; display: none;">
                    <div style="font-size: 24px; font-weight: 900; color: white; display: flex; align-items: center; gap: 12px;">
                        <span style="background:white; color:var(--sidebar-bg); width:36px; height:36px; border-radius:10px; display:flex; align-items:center; justify-content:center; font-size:18px;">E</span>
                        ELIM APP
                    </div>
                </div>
            `;

            nav.innerHTML = brandHtml + tabs.map(tab => `
                <a href="${tab.link}" class="nav-item ${activeId === tab.id ? 'active' : ''}">
                    <span class="nav-icon">${tab.icon}</span>
                    <span class="nav-label">${tab.label}</span>
                </a>
            `).join('');
            return nav;
        }
    },

    bindLoginEvents() {
        const form = document.getElementById('loginForm');
        if (form) {
            form.onsubmit = async (e) => {
                e.preventDefault();
                const u = document.getElementById('username').value;
                const p = document.getElementById('password').value;
                await this.login(u, p);
            };
        }
    },

    bindModeSelectorEvents() {
        document.querySelectorAll('.mode-btn').forEach(btn => {
            btn.onclick = () => {
                const mode = btn.getAttribute('data-mode');
                this.state.activeMode = mode;
                sessionStorage.setItem('elim_active_mode', mode);
                window.location.hash = '#/dashboard';
            };
        });
        const out = document.getElementById('modeSelectorLogout');
        if (out) out.onclick = () => this.logout();
    },

    bindDashboardEvents() {
        const out = document.getElementById('logoutBtn');
        if (out) out.onclick = () => this.logout();
        const sw = document.getElementById('switchModeBtn');
        if (sw) {
            sw.onclick = () => {
                this.state.activeMode = null;
                sessionStorage.removeItem('elim_active_mode');
                window.location.hash = '#/select-mode';
            };
        }
    },

    bindAdminEvents(allProfiles = []) {
        // Enfoque híbrido: Bindings directos para formularios (seguros por mountView)
        // y delegación para botones dinámicos en listas
        const appDiv = document.getElementById('app');
        this.state.allProfilesRaw = allProfiles;

        // 1. Formulario: Crear Persona
        const personForm = document.getElementById('personForm');
        if (personForm) {
            personForm.onsubmit = async (e) => {
                e.preventDefault();
                const nameInput = document.getElementById('personName');
                const leaderInput = document.getElementById('assignToUser');
                const name = nameInput.value;
                const leaderId = leaderInput.value;

                if (!name || !leaderId) return this.notify('Por favor completa todos los campos', 'error');

                const btn = e.target.querySelector('button[type="submit"]');
                btn.disabled = true;
                btn.innerHTML = '<span class="spinner-sm"></span> Guardando...';

                try {
                    const success = await this.savePerson(name, leaderId);
                    if (success) {
                        this.notify('✅ Persona creada y asignada');
                        nameInput.value = '';
                        this.state.cache.metadata = null; // Invalida caché para ver cambios inmediatos
                        await this.loadAdminData(true); // silent refresh
                    }
                } catch (err) {
                    this.notify('❌ Error al crear persona: ' + err.message, 'error');
                } finally {
                    btn.disabled = false;
                    btn.innerHTML = 'Crear y Asignar';
                }
            };
        }

        // 2. Formulario: Mover Discipulador
        const sectorForm = document.getElementById('userSectorForm');
        if (sectorForm) {
            sectorForm.onsubmit = async (e) => {
                e.preventDefault();
                const userId = document.getElementById('usUserSelect').value;
                const sectorId = document.getElementById('usSectorSelect').value;

                if (!userId || !sectorId) return this.notify('Selecciona el discipulador y el sector', 'error');

                const btn = e.target.querySelector('button[type="submit"]');
                btn.disabled = true;
                btn.innerHTML = '<span class="spinner-sm"></span> Moviendo...';

                try {
                    const success = await this.saveUserSector(userId, sectorId);
                    if (success) {
                        this.notify('✅ Sector actualizado');
                        this.state.cache.metadata = null;
                        await this.loadAdminData(true); // silent refresh
                    }
                } catch (err) {
                    this.notify('❌ Error al mover discipulador: ' + err.message, 'error');
                } finally {
                    btn.disabled = false;
                    btn.innerHTML = 'Confirmar Reubicación';
                }
            };
        }

        // 3. Delegación para botones de lista (Reasignar y Desasignar)
        // Usamos delegación en appDiv porque estas listas pueden ser muy largas
        if (this._adminDelegationBound) return;
        this._adminDelegationBound = true;

        appDiv.addEventListener('click', async (e) => {
            if (window.location.hash !== '#/admin') return;

            const target = e.target.closest('.reassign-btn, .unassign-btn, .delete-sheep-btn');
            if (!target) return;

            const id = target.getAttribute('data-id');
            const name = target.getAttribute('data-name');

            if (target.classList.contains('unassign-btn')) {
                if (await this.confirmDialog('¿Liberar Persona?', `¿Estás seguro que deseas desasignar a ${name}?`, 'Liberar')) {
                    try {
                        await this.unassignPerson(id);
                        this.notify(`✅ ${name} ha sido movido a la lista de pendientes.`);
                        this.state.cache.metadata = null;
                        await this.loadAdminData(true); // silent refresh
                    } catch (err) {
                        this.notify(`❌ Falló la liberación: ${err.message}`, 'error');
                    }
                }
            }

            if (target.classList.contains('delete-sheep-btn')) {
                if (await this.confirmDialog('¿Eliminar Persona?', `¿Confirmas que deseas eliminar permanentemente a ${name}? Esta acción no se puede deshacer.`, 'Sí, eliminar')) {
                    try {
                        const success = await this.deletePerson(id);
                        if (success) {
                            this.notify(`🗑️ ${name} ha sido eliminado.`);
                            this.state.cache.metadata = null;
                            await this.loadAdminData(true); // silent refresh
                        }
                    } catch (err) {
                        this.notify(`❌ Error al eliminar: ${err.message}`, 'error');
                    }
                }
            }

            if (target.classList.contains('reassign-btn')) {
                const leaderOptions = (this.state.allProfilesRaw || [])
                    .filter(p => [p.role, ...(p.additional_roles || [])].some(r => ['pastor', 'coordinador', 'discipulador', 'pastorexterno'].includes(r)))
                    .map(p => ({ value: p.id, label: p.name }));

                const newId = await this.promptDialog('Reasignar Responsable', `Selecciona el nuevo discipulador para ${name}:`, leaderOptions);

                if (newId) {
                    try {
                        const success = await this.reassignPerson(id, newId);
                        if (success) {
                            this.notify('✅ Reasignación completada');
                            this.state.cache.metadata = null;
                            await this.loadAdminData(true); // silent refresh
                        }
                    } catch (err) {
                        this.notify('❌ Error al reasignar: ' + err.message, 'error');
                    }
                }
            }
        });

        // 4. Guardar Semanas Libres
        const btnSaveFreeWeeks = document.getElementById('btnSaveFreeWeeks');
        if (btnSaveFreeWeeks) {
            btnSaveFreeWeeks.onclick = async () => {
                const checkboxes = document.querySelectorAll('.free-week-checkbox');
                const checkedDates = Array.from(checkboxes).filter(cb => cb.checked).map(cb => cb.value);

                btnSaveFreeWeeks.disabled = true;
                btnSaveFreeWeeks.innerHTML = '<span class="spinner-sm"></span> Guardando...';

                try {
                    const currentYear = new Date().getFullYear();
                    const yearStart = `${currentYear}-01-01`;
                    const yearEnd = `${currentYear}-12-31`;

                    // 1. Borrar registros del año actual
                    await window.supabaseClient
                        .from('free_weeks')
                        .delete()
                        .gte('saturday_date', yearStart)
                        .lte('saturday_date', yearEnd);

                    // 2. Insertar nuevos registros
                    if (checkedDates.length > 0) {
                        const payload = checkedDates.map(d => ({ saturday_date: d, description: 'Semana Libre Administrativa' }));
                        const { error: insErr } = await window.supabaseClient
                            .from('free_weeks')
                            .insert(payload);

                        if (insErr) throw insErr;
                    }

                    this.state.cache.metadata = null;
                    this.notify('✅ Configuración de semanas libres guardada correctamente');
                    await this.loadAdminData();
                } catch (err) {
                    console.error('Error guardando semanas libres:', err);
                    this.notify('❌ Error al guardar. Asegúrate de que la tabla free_weeks exista en Supabase.', 'error');
                } finally {
                    btnSaveFreeWeeks.disabled = false;
                    btnSaveFreeWeeks.innerHTML = 'Guardar Configuración Global';
                }
            };
        }
    },

    bindEstructuraEvents() {
        // Por ahora informativa, pero se pueden añadir filtros de búsqueda local si se desea
    },

    bindReportEvents() {
        const dateInput = document.getElementById('reportDate');
        if (dateInput) {
            dateInput.addEventListener('change', (e) => {
                const newDate = e.target.value;
                // Si estamos en modo edición, al cambiar de semana reseteamos el hash a #/reporte
                // para que el sistema cargue la lógica de reporte nuevo/borrador de esa semana.
                if (window.location.hash.startsWith('#/editar-reporte/')) {
                    this.state._targetDate = newDate; // Paso de parámetro temporal
                    window.location.hash = '#/reporte';
                } else {
                    this.renderReportView(newDate);
                }
            });
        }

        const updateUIState = () => {
            const rows = document.querySelectorAll('.person-row-ref');
            let completed = 0;
            rows.forEach(row => {
                if (row.getAttribute('data-fri') !== 'unanswered' && row.getAttribute('data-sat') !== 'unanswered') {
                    completed++;
                }
            });

            const percent = rows.length > 0 ? (completed / rows.length) * 100 : 0;
            const currentPercent = percent.toFixed(0) + '%';
            const bar = document.getElementById('progressBar');
            if (bar) bar.style.width = currentPercent;

            const stickyBar = document.getElementById('stickyProgressBar');
            if (stickyBar) stickyBar.style.width = currentPercent;

            const stickyText = document.getElementById('stickyProgressText');
            if (stickyText) stickyText.innerText = currentPercent;

            const text = document.getElementById('progressText');
            if (text) text.innerText = `${completed} / ${rows.length}`;

            const dateValue = document.getElementById('reportDate').value;
            const status = this.getReportStatus(dateValue, percent);

            const badge = document.getElementById('statusBadge');
            if (badge) {
                badge.innerText = status.label;
                badge.style.background = status.color;
            }

            const blockerMsg = document.getElementById('blockerMsg');
            if (blockerMsg) {
                blockerMsg.style.display = status.msg ? 'flex' : 'none';
                const bText = document.getElementById('blockerText');
                if (bText) bText.innerText = status.msg;
            }

            const sendBtn = document.getElementById('sendReportBtn');
            if (sendBtn) {
                const isEdit = document.getElementById('reportDate').hasAttribute('data-edit-id');
                sendBtn.disabled = !status.canSend;
                sendBtn.innerText = status.canSend ? (isEdit ? 'Guardar modificación' : 'Enviar Reporte') : (status.msg || 'Pendiente');
            }
        };

        const setActiveBtn = (btnActive, btnInactive) => {
            btnActive.classList.add('active');
            btnInactive.classList.remove('active');
        };

        document.querySelectorAll('.person-row-ref').forEach(row => {
            ['fri', 'sat'].forEach(day => {
                const btnPresent = row.querySelector(`.btn-${day}-present`);
                const btnAbsent = row.querySelector(`.btn-${day}-absent`);

                if (btnPresent && btnAbsent) {
                    btnPresent.onclick = (e) => {
                        e.stopPropagation();
                        row.setAttribute(`data-${day}`, 'present');
                        setActiveBtn(btnPresent, btnAbsent);
                        updateUIState();
                    };

                    btnAbsent.onclick = (e) => {
                        e.stopPropagation();
                        row.setAttribute(`data-${day}`, 'absent');
                        setActiveBtn(btnAbsent, btnPresent);
                        updateUIState();
                    };
                }
            });
        });

        const sendBtnEl = document.getElementById('sendReportBtn');
        if (sendBtnEl) sendBtnEl.onclick = () => this.submitReport(false);

        const draftBtn = document.getElementById('saveDraftBtn');
        if (draftBtn) draftBtn.onclick = () => this.submitReport(true);

        updateUIState();
    },

    async loadReporteSector(filters = {}) {
        if (!this.state.user) return;

        const user = this.state.user;
        const activeMode = this.state.activeMode || user.role;
        const isPastor = user.role === 'pastor';
        const isCoord = activeMode === 'coordinador';

        if (!isPastor && !isCoord) {
            window.location.hash = '#/dashboard';
            return;
        }

        this.mountView('reporte-sector', this.views.loadingState('Generando reporte por sector...'));

        try {
            const meta = await this.getGlobalMeta();
            const weeks = this.getAvailableWeeks();

            // Determinar sector y fecha por defecto
            if (!filters.sector_id) {
                filters.sector_id = isPastor ? meta.sectors[0]?.id : meta.profiles.find(p => p.id === user.id)?.sector_id;
            }
            if (!filters.report_date) {
                filters.report_date = weeks[0].dateStr;
            }

            const currentSector = meta.sectors.find(s => s.id === filters.sector_id) || { name: 'Sector Desconocido' };
            const coordinator = currentSector.coordinator || { name: 'Sin asignar' };

            // 1. Obtener discipuladores del sector
            const discipliners = meta.profiles.filter(p => p.sector_id === filters.sector_id && this.isReporter(p, meta));
            const disciplinerIds = discipliners.map(d => d.id);

            // 2. Obtener reportes de la semana
            const reports = await this.safeCall(() =>
                window.supabaseClient
                    .from('reports')
                    .select('id, reporter_id, attendances(person_id, attended_friday, attended_saturday)')
                    .eq('report_date', filters.report_date)
                    .in('reporter_id', disciplinerIds)
            );

            // 3. Procesar datos de la tabla
            const disciplinerData = discipliners.map(d => {
                const report = (reports || []).find(r => r.reporter_id === d.id);
                let friAtt = 0, friAbs = 0, satAtt = 0, satAbs = 0;

                if (report && report.attendances) {
                    report.attendances.forEach(a => {
                        if (a.attended_friday) friAtt++; else friAbs++;
                        if (a.attended_saturday) satAtt++; else satAbs++;
                    });
                }

                return { name: d.name, friAtt, friAbs, satAtt, satAbs };
            });

            const totals = disciplinerData.reduce((acc, d) => ({
                friAtt: acc.friAtt + d.friAtt,
                friAbs: acc.friAbs + d.friAbs,
                satAtt: acc.satAtt + d.satAtt,
                satAbs: acc.satAbs + d.satAbs
            }), { friAtt: 0, friAbs: 0, satAtt: 0, satAbs: 0 });

            const calcPerc = (val, total) => total > 0 ? ((val / total) * 100).toFixed(1) : '0.0';
            const percentages = {
                friAtt: calcPerc(totals.friAtt, totals.friAtt + totals.friAbs),
                friAbs: calcPerc(totals.friAbs, totals.friAtt + totals.friAbs),
                satAtt: calcPerc(totals.satAtt, totals.satAtt + totals.satAbs),
                satAbs: calcPerc(totals.satAbs, totals.satAtt + totals.satAbs)
            };

            // 4. Lista de Seguimiento (Módulo Actual)
            const currentModule = this.getModuleForDate(filters.report_date);
            let followUp = [];

            if (currentModule) {
                // Traer todos los reportes del sector en el módulo
                const moduleReports = await this.safeCall(() =>
                    window.supabaseClient
                        .from('reports')
                        .select('id, reporter_id, attendances(person_id, attended_friday, attended_saturday, people_assigned(name))')
                        .gte('report_date', currentModule.startSat.toISOString().split('T')[0])
                        .lte('report_date', currentModule.endSat.toISOString().split('T')[0])
                        .in('reporter_id', disciplinerIds)
                );

                const sheepFaults = {};
                (moduleReports || []).forEach(r => {
                    const discipulador = discipliners.find(d => d.id === r.reporter_id);
                    (r.attendances || []).forEach(a => {
                        if (!a.attended_friday && !a.attended_saturday) {
                            if (!sheepFaults[a.person_id]) {
                                sheepFaults[a.person_id] = {
                                    name: a.people_assigned?.name || 'Desconocido',
                                    discipliner: discipulador?.name || 'Sin asignar',
                                    absences: 0
                                };
                            }
                            sheepFaults[a.person_id].absences++;
                        }
                    });
                });

                followUp = Object.values(sheepFaults)
                    .filter(s => s.absences >= 5)
                    .sort((a, b) => b.absences - a.absences);
            }

            const viewData = {
                sector: currentSector,
                coordinator,
                week: { dateStr: filters.report_date },
                discipliners: disciplinerData,
                totals,
                percentages,
                followUp,
                filters,
                sectors: meta.sectors,
                weeks
            };

            if (window.location.hash === '#/reporte-sector') {
                this.mountView('reporte-sector', this.views.reporteSector(viewData));
                this.bindReporteSectorEvents(filters);
            }

        } catch (err) {
            console.error('[Sector Report Error]', err);
            this.mountView('reporte-sector', this.views.errorState(err.message || 'Error al generar reporte de sector'));
        }
    },

    bindReporteSectorEvents(filters) {
        const rsSector = document.getElementById('rsSector');
        const rsWeek = document.getElementById('rsWeek');

        if (rsSector) rsSector.onchange = () => this.loadReporteSector({ ...filters, sector_id: rsSector.value });
        if (rsWeek) rsWeek.onchange = () => this.loadReporteSector({ ...filters, report_date: rsWeek.value });

        const btnDownload = document.getElementById('btnDownloadSectorReport');
        if (btnDownload) {
            btnDownload.onclick = () => this.downloadSectorReportPdf();
        }
    },

    async downloadSectorReportPdf() {
        const element = document.querySelector('.dashboard-content');
        if (!element) return;

        const sectorName = document.querySelector('.header-user h2')?.innerText || 'Sector';
        const dateStr = document.getElementById('rsWeek')?.value || 'Semana';

        const opt = {
            margin: [10, 10, 10, 10],
            filename: `Reporte_Sector_${sectorName.replace(/\s+/g, '_')}_${dateStr}.pdf`,
            image: { type: 'jpeg', quality: 0.98 },
            html2canvas: { scale: 2, useCORS: true, letterRendering: true },
            jsPDF: { unit: 'mm', format: 'a4', orientation: 'portrait' }
        };

        this.notify('Generando PDF del Sector...', 'info');

        try {
            // Clonar sin los filtros para que el PDF se vea más limpio
            const clone = element.cloneNode(true);
            const filtersBlock = clone.querySelector('.glass.animate-reveal');
            if (filtersBlock) filtersBlock.remove();

            const header = document.createElement('div');
            header.innerHTML = `
                <div style="text-align:center; padding-bottom:20px; border-bottom:2px solid #f1f5f9; margin-bottom:20px;">
                    <h1 style="color:#0F172A; margin:0;">Reporte Semanal por Sector</h1>
                    <p style="color:#64748B; margin:5px 0 0 0;">Iglesia Elim - Gestión de Discipulado</p>
                </div>
            `;
            clone.prepend(header);

            await window.html2pdf().set(opt).from(clone).save();
            this.notify('✅ Reporte del Sector descargado');
        } catch (err) {
            console.error('Error generando PDF Sector:', err);
            this.notify('❌ Error al generar PDF', 'error');
        }
    },

    async loadEstadisticas(filters = {}) {
        if (!this.state.user) return;

        const user = this.state.user;
        const activeMode = this.state.activeMode || user.role;
        const isPastor = user.role === 'pastor';
        const isCoord = activeMode === 'coordinador';

        // 1. Mostrar estado de carga si es el primer render
        if (!document.getElementById('statsPrintArea')) {
            this.mountView('estadisticas', this.views.loadingState('Analizando estadísticas pastorales...'), 'estadisticas');
        }

        try {
            const meta = await this.getGlobalMeta();

            let query = window.supabaseClient.from('reports').select('id, report_date, reporter_id, attendances(person_id, attended_friday, attended_saturday)').order('report_date', { ascending: true });
            if (!isPastor) {
                if (isCoord) {
                    const coordSectors = meta.sectors.filter(s => s.coordinator_id === user.id).map(s => s.id);
                    const allowedLeaders = meta.discipliners.filter(d => coordSectors.includes(d.sector_id)).map(d => d.id);
                    allowedLeaders.push(user.id);
                    if (allowedLeaders.length > 0) {
                        query = query.in('reporter_id', allowedLeaders);
                    } else {
                        query = query.eq('reporter_id', user.id);
                    }
                } else {
                    query = query.eq('reporter_id', this.state.session.user.id);
                }
            } else {
                if (filters.reporter_id) query = query.eq('reporter_id', filters.reporter_id);
                if (filters.sector_id) {
                    const profilesInSector = meta.discipliners.filter(p => p.sector_id === filters.sector_id).map(p => p.id);
                    if (profilesInSector.length > 0) query = query.in('reporter_id', profilesInSector);
                }
            }

            const rawReports = await this.safeCall(() => query);

            // 4. Filtrar Metadata (Cascada de filtros)
            let displayMeta = { ...meta };
            if (filters.sector_id) {
                displayMeta.discipliners = meta.discipliners.filter(d => d.sector_id === filters.sector_id);
                if (filters.reporter_id && !displayMeta.discipliners.find(d => d.id === filters.reporter_id)) {
                    filters.reporter_id = '';
                }
            }
            if (filters.reporter_id) {
                displayMeta.sheep = meta.sheep.filter(s => s.assigned_to === filters.reporter_id);
                if (filters.person_id && !displayMeta.sheep.find(s => s.id === filters.person_id)) {
                    filters.person_id = '';
                }
            }

            // 5. Procesar
            const stats = this.processStats(rawReports || [], filters, meta);

            // 6. Renderizado inteligente (solo si seguimos en la vista)
            if (window.location.hash !== '#/estadisticas') return;

            const existingArea = document.getElementById('statsPrintArea');
            if (existingArea) {
                // Actualización parcial para mejor rendimiento
                const rateEl = document.getElementById('statAttendanceRate');
                if (rateEl) {
                    rateEl.innerText = `${stats.attendanceRate}%`;
                    const bar = document.getElementById('statAttendanceProgress');
                    if (bar) bar.style.width = `${stats.attendanceRate}%`;
                }
                const fAttEl = document.getElementById('statFriAttended');
                if (fAttEl) fAttEl.innerText = stats.friAttended;
                const fAbsEl = document.getElementById('statFriAbsent');
                if (fAbsEl) fAbsEl.innerText = stats.friAbsent;
                const sAttEl = document.getElementById('statSatAttended');
                if (sAttEl) sAttEl.innerText = stats.satAttended;
                const sAbsEl = document.getElementById('statSatAbsent');
                if (sAbsEl) sAbsEl.innerText = stats.satAbsent;

                const reportsEl = document.getElementById('statReportsCount');
                if (reportsEl) reportsEl.innerText = stats.totalReports;
                const totalAttEl = document.getElementById('statTotalAttended');
                if (totalAttEl) totalAttEl.innerText = stats.totalAttended;
                const totalAbsEl = document.getElementById('statTotalAbsent');
                if (totalAbsEl) totalAbsEl.innerText = stats.totalAbsent;

                // Matriz
                const matrixBody = document.getElementById('attendanceMatrixBody');
                if (matrixBody) {
                    matrixBody.innerHTML = stats.matrix.rows.map(row => this.renderMatrixRow(row, stats.matrix.dates)).join('');
                }

                // Charts & Dropdowns
                this.renderSplitStatsCharts(stats.detailsData, stats.averageData);
                this.updateStatsDropdowns(displayMeta, filters);
            } else {
                // Renderizado completo inicial
                this.mountView('estadisticas', this.views.estadisticas(stats, displayMeta, filters), 'estadisticas');

                // Usamos requestAnimationFrame para asegurar que el navegador ha pintado el HTML
                // antes de que Chart.js intente acceder a los elementos canvas.
                requestAnimationFrame(() => {
                    this.renderSplitStatsCharts(stats.detailsData, stats.averageData);
                    this.bindStatsEvents();
                });
            }
        } catch (err) {
            console.error('[Stats Load Error]', err);
            this.mountView('estadisticas', this.views.errorState(err.message || 'Error analizando estadísticas'), 'estadisticas');
        }
    },

    renderMatrixRow(row, dates) {
        const nameParts = row.name.split(' ');
        const firstName = nameParts[0] || '';
        const lastName = nameParts.slice(1).join(' ') || '';

        return `
            <tr>
                <td class="sticky-column-cell" style="padding:12px 14px; background:#f8fafc; border-radius:16px 0 0 16px; position: sticky; left: 0; z-index: 10; box-shadow: 4px 0 8px rgba(0,0,0,0.03); min-width: 110px; max-width: 130px;">
                    <div style="display:flex; align-items:center; gap:8px;">
                        <div class="sector-avatar sm" style="background:var(--primary); color:white; font-size:9px; width:28px; height:28px; min-width:28px;">${firstName.charAt(0)}${lastName.charAt(0)}</div>
                        <div style="display:flex; flex-direction:column; line-height:1.1;">
                            <div style="font-size:10px; color:var(--text-muted); font-weight:600; text-transform:uppercase; letter-spacing:0.02em;">${firstName}</div>
                            <div style="font-weight:800; font-size:11px; color:var(--primary); overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">${lastName}</div>
                        </div>
                    </div>
                </td>
                ${row.attendances.map((att, idx) => {
            if (att === null) return `<td style="text-align:center; color:#cbd5e1; font-size:14px; background:#f8fafc; font-weight:800;">—</td>`;

            const isLast = idx === row.attendances.length - 1;
            const dateStr = dates[idx];
            const { friday, saturday } = App.getReportWeekRange(dateStr);
            const month = saturday.toLocaleDateString('es-ES', { month: 'short' });
            const dateLabel = `${friday.getDate()}-${saturday.getDate()} ${month}`;

            return `
                        <td style="text-align:center; background:#f8fafc; padding: 12px 10px; ${isLast ? 'border-radius:0 16px 16px 0;' : ''}">
                            <div style="display:flex; gap:16px; justify-content:center; align-items: center;">
                                <div class="pill ${att.v ? 'pill-success' : 'pill-danger'}" style="width:32px; height:32px; border-radius:50%; display:inline-block; line-height:32px; text-align:center; font-size:11px; font-weight:900; padding:0 !important; box-shadow: 0 4px 10px rgba(0,0,0,0.08); transition: transform 0.2s;">V</div>
                                <div class="pill ${att.s ? 'pill-success' : 'pill-danger'}" style="width:32px; height:32px; border-radius:50%; display:inline-block; line-height:32px; text-align:center; font-size:11px; font-weight:900; padding:0 !important; box-shadow: 0 4px 10px rgba(0,0,0,0.08); transition: transform 0.2s;">S</div>
                            </div>
                        </td>
                    `;
        }).join('')}
            </tr>
        `;
    },

    updateStatsDropdowns(meta, filters) {
        const stLeader = document.getElementById('stLeader');
        const stSheep = document.getElementById('stSheep');

        if (stLeader) {
            const currentLeader = stLeader.value;
            stLeader.innerHTML = '<option value="">Cualquier Discipulador</option>' +
                meta.discipliners.filter(l => App.isReporter(l, meta)).map(l => `<option value="${l.id}" ${filters.reporter_id === l.id ? 'selected' : ''}>${l.name}</option>`).join('');
        }
        if (stSheep) {
            stSheep.innerHTML = '<option value="">Todas las Personas</option>' +
                meta.sheep.map(p => `<option value="${p.id}" ${filters.person_id === p.id ? 'selected' : ''}>${p.name}</option>`).join('');
        }
    },

    processStats(reports, filters, meta) {
        // Establecer el SCOPE de ovejas permitidas ANTES de iterar para no mezclar con ajenas o eliminadas
        let scopeSheep = meta.sheep;
        const user = App.state.user;
        const activeMode = App.state.activeMode || user.role;
        const isPastor = user.role === 'pastor';
        const isCoord = activeMode === 'coordinador';

        if (!isPastor) {
            if (isCoord) {
                const coordSectors = meta.sectors.filter(s => s.coordinator_id === user.id).map(s => s.id);
                const allowedLeaders = meta.discipliners.filter(d => coordSectors.includes(d.sector_id)).map(d => d.id);
                allowedLeaders.push(user.id);
                scopeSheep = scopeSheep.filter(s => allowedLeaders.includes(s.assigned_to));
            } else {
                scopeSheep = scopeSheep.filter(s => s.assigned_to === user.id);
            }
        }

        if (filters.person_id) {
            scopeSheep = scopeSheep.filter(s => s.id === filters.person_id);
        } else if (filters.reporter_id) {
            scopeSheep = scopeSheep.filter(s => s.assigned_to === filters.reporter_id);
        } else if (filters.sector_id) {
            const sectorDiscipliners = meta.discipliners.filter(d => d.sector_id === filters.sector_id).map(d => d.id);
            scopeSheep = scopeSheep.filter(s => sectorDiscipliners.includes(s.assigned_to));
        }

        const scopeSheepIds = scopeSheep.map(s => s.id);

        // Enriquecer reportes respetando la inmutabilidad correcta
        const enriched = reports.map(r => {
            const { friday } = this.getReportWeekRange(r.report_date);
            return {
                ...r,
                attendances: Array.isArray(r.attendances) ? r.attendances : [],
                weekKey: friday.toISOString().split('T')[0]
            };
        });

        let filtered = enriched;
        if (filters.sector_id) {
            const leaderIds = meta.discipliners.filter(p => p.sector_id === filters.sector_id).map(p => p.id);
            filtered = filtered.filter(r => leaderIds.includes(r.reporter_id));
        }
        if (filters.reporter_id) {
            filtered = filtered.filter(r => r.reporter_id === filters.reporter_id);
        }

        let totalExpected = 0;
        let friAttended = 0; let friAbsent = 0;
        let satAttended = 0; let satAbsent = 0;

        const timeline = {};

        filtered.forEach(r => {
            const wk = r.weekKey;
            if (!timeline[wk]) timeline[wk] = { expected: 0, fri: 0, sat: 0 };

            r.attendances.forEach(a => {
                // EXCLUSIVO: Solo iteramos si la persona actualmente pertenece al scope del usuario y filtro actual
                if (!scopeSheepIds.includes(a.person_id)) return;

                totalExpected++;
                if (a.attended_friday) friAttended++; else friAbsent++;
                if (a.attended_saturday) satAttended++; else satAbsent++;

                timeline[wk].expected++;
                if (a.attended_friday) timeline[wk].fri++;
                if (a.attended_saturday) timeline[wk].sat++;
            });
        });

        const totalMeetingsExpected = totalExpected * 2;
        const totalAttended = friAttended + satAttended;
        const totalAbsent = friAbsent + satAbsent;

        const attendanceRate = totalMeetingsExpected > 0 ? ((totalAttended / totalMeetingsExpected) * 100).toFixed(1) : 0;
        const friRate = totalExpected > 0 ? ((friAttended / totalExpected) * 100).toFixed(1) : 0;
        const satRate = totalExpected > 0 ? ((satAttended / totalExpected) * 100).toFixed(1) : 0;

        // 4. Matriz de Asistencia (Agrupada por Semana)
        const uniqueWeeks = [...new Set(filtered.map(r => r.weekKey))].sort().reverse().slice(0, 5);

        const matrixRows = scopeSheep.map(person => {
            const personAttendances = uniqueWeeks.map(wk => {
                // Buscar en TODOS los reportes de esa semana filtrados
                const weekReports = filtered.filter(r => r.weekKey === wk);
                // Buscar si la persona aparece en alguno de los reportes de esa semana
                let attend = null;
                for (const r of weekReports) {
                    const a = r.attendances.find(att => att.person_id === person.id);
                    if (a) {
                        attend = { v: a.attended_friday, s: a.attended_saturday };
                        break; // Ya encontramos su registro para esta semana
                    }
                }
                return attend;
            });
            return { name: person.name, attendances: personAttendances };
        });

        const sortedWeeks = Object.keys(timeline).sort();

        const averageData = {
            labels: sortedWeeks,
            datasets: [{
                label: 'Promedio General',
                data: sortedWeeks.map(wk => timeline[wk].expected > 0 ? ((timeline[wk].fri + timeline[wk].sat) / (timeline[wk].expected * 2) * 100).toFixed(1) : 0),
                borderColor: '#6366F1',
                backgroundColor: 'rgba(99, 102, 241, 0.15)',
                fill: true,
                tension: 0.4,
                borderWidth: 3,
                pointRadius: 4,
                pointBackgroundColor: '#fff'
            }]
        };

        const detailsData = {
            labels: sortedWeeks,
            datasets: [
                {
                    type: 'bar',
                    label: 'Viernes',
                    data: sortedWeeks.map(wk => timeline[wk].expected > 0 ? (timeline[wk].fri / timeline[wk].expected * 100).toFixed(1) : 0),
                    backgroundColor: '#3B82F6',
                    borderRadius: 8,
                    barPercentage: 0.7,
                    categoryPercentage: 0.6
                },
                {
                    type: 'bar',
                    label: 'Sábado',
                    data: sortedWeeks.map(wk => timeline[wk].expected > 0 ? (timeline[wk].sat / timeline[wk].expected * 100).toFixed(1) : 0),
                    backgroundColor: '#10B981',
                    borderRadius: 8,
                    barPercentage: 0.7,
                    categoryPercentage: 0.6
                }
            ]
        };

        const matrix = { dates: uniqueWeeks, rows: matrixRows };

        return { attendanceRate, friRate, satRate, totalReports: filtered.length, totalExpected, totalAttended, totalAbsent, friAttended, friAbsent, satAttended, satAbsent, averageData, detailsData, matrix };
    },

    renderSplitStatsCharts(detailsData, averageData) {
        const ctxDetails = document.getElementById('detailsChart');
        const ctxAverage = document.getElementById('averageChart');

        // Si los elementos no existen aún, reintentamos en el siguiente frame (máxima resiliencia)
        if (!ctxDetails || !ctxAverage) {
            requestAnimationFrame(() => this.renderSplitStatsCharts(detailsData, averageData));
            return;
        }

        // Limpieza de instancias previas para evitar gráficas fantasma o vacías
        if (this._detailsChart && typeof this._detailsChart.destroy === 'function') {
            this._detailsChart.destroy();
        }
        if (this._averageChart && typeof this._averageChart.destroy === 'function') {
            this._averageChart.destroy();
        }

        // Formatear etiquetas de fecha a algo más legible (ej: 12 Abr)
        const formatLabel = (dateStr) => {
            const d = new Date(dateStr + 'T12:00:00');
            return d.toLocaleDateString('es-ES', { day: 'numeric', month: 'short' });
        };

        if (detailsData.labels) detailsData.labels = detailsData.labels.map(formatLabel);
        if (averageData.labels) averageData.labels = averageData.labels.map(formatLabel);

        const commonOptions = {
            responsive: true,
            maintainAspectRatio: false,
            animation: {
                duration: 1000,
                easing: 'easeInOutQuart'
            },
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    align: 'end',
                    labels: {
                        boxWidth: 8,
                        usePointStyle: true,
                        font: { size: 10, weight: '600' }
                    }
                },
                tooltip: {
                    backgroundColor: '#fff',
                    titleColor: '#1e293b',
                    bodyColor: '#475569',
                    borderColor: '#e2e8f0',
                    borderWidth: 1,
                    padding: 12,
                    displayColors: true,
                    callbacks: { label: (c) => ` ${c.dataset.label}: ${c.parsed.y}%` }
                }
            },
            scales: {
                y: {
                    min: 0,
                    max: 100,
                    ticks: {
                        stepSize: 25,
                        callback: v => v + '%',
                        font: { size: 10 }
                    },
                    grid: { color: 'rgba(0,0,0,0.03)', drawBorder: false }
                },
                x: {
                    grid: { display: false },
                    ticks: { font: { size: 10 } }
                }
            }
        };

        // Gráfica de Barras (Detalles)
        this._detailsChart = new Chart(ctxDetails, {
            type: 'bar',
            data: detailsData,
            options: commonOptions
        });

        // Gráfica de Línea (Promedio) - Con Gradiente
        const gradCtx = ctxAverage.getContext('2d');
        const gradient = gradCtx.createLinearGradient(0, 0, 0, 300);
        gradient.addColorStop(0, 'rgba(99, 102, 241, 0.2)');
        gradient.addColorStop(1, 'rgba(99, 102, 241, 0.0)');
        averageData.datasets[0].backgroundColor = gradient;

        this._averageChart = new Chart(ctxAverage, {
            type: 'line',
            data: averageData,
            options: {
                ...commonOptions,
                plugins: {
                    ...commonOptions.plugins,
                    legend: { display: false }
                }
            }
        });
    },

    bindStatsEvents() {
        const stSector = document.getElementById('stSector');
        const stLeader = document.getElementById('stLeader');
        const stSheep = document.getElementById('stSheep');

        const getFilters = () => ({
            sector_id: stSector ? stSector.value : '',
            reporter_id: stLeader ? stLeader.value : '',
            person_id: stSheep ? stSheep.value : ''
        });

        [stSector, stLeader, stSheep].forEach(el => {
            if (el) el.onchange = () => this.loadEstadisticas(getFilters());
        });

        const btnDownload = document.getElementById('btnDownloadStats');
        if (btnDownload) {
            btnDownload.onclick = () => this.downloadReportPDF();
        }
    },

    async downloadReportPDF() {
        if (typeof html2pdf === 'undefined') {
            this.notify('❌ Error: Librería de PDF no cargada. Por favor recarga la página.', 'error');
            return;
        }

        const area = document.getElementById('statsPrintArea');
        if (!area) return;

        this.notify('✨ Generando Reporte Pastoral...', 'info');

        // 1. Obtener datos básicos
        const sector = document.getElementById('stSector')?.options[document.getElementById('stSector').selectedIndex]?.text || 'General';
        const leader = document.getElementById('stLeader')?.options[document.getElementById('stLeader').selectedIndex]?.text || 'Todos';
        const sheep = document.getElementById('stSheep')?.options[document.getElementById('stSheep').selectedIndex]?.text || 'Todas';

        // 2. Capturar gráficas como Base64
        let chart1Img = '';
        let chart2Img = '';
        try {
            const c1 = document.getElementById('detailsChart');
            const c2 = document.getElementById('averageChart');
            if (c1) chart1Img = c1.toDataURL('image/png', 1.0);
            if (c2) chart2Img = c2.toDataURL('image/png', 1.0);
        } catch (e) {
            console.error('Error capturando gráficas:', e);
        }

        // 3. Obtener tabla de asistencia y sanearla para PDF
        const table = document.querySelector('.attendance-matrix-table');
        let tableHtml = '';
        if (table) {
            const clone = table.cloneNode(true);
            // Limpiar estilos de UI que interfieren con el PDF
            clone.style.width = '100%';
            clone.style.minWidth = '100%';
            clone.style.borderSpacing = '0 3px';
            clone.style.margin = '0';

            clone.querySelectorAll('.sticky-column-header, .sticky-column-cell').forEach(el => {
                el.style.position = 'static';
                el.style.boxShadow = 'none';
                el.style.background = 'transparent';
                el.style.width = 'auto';
            });
            tableHtml = clone.outerHTML;
        } else {
            tableHtml = '<p style="text-align:center; color:#94a3b8; padding: 20px;">No hay datos de asistencia para el período seleccionado.</p>';
        }

        // 4. Construir HTML completo del Reporte
        const reportTemplate = `
            <div style="font-family: Arial, sans-serif; padding: 40px; color: #1e293b; background: #ffffff; width: 850px; margin: 0 auto; box-sizing: border-box;">
                <div style="text-align: center; border-bottom: 5px solid #2563eb; padding-bottom: 25px; margin-bottom: 35px;">
                    <h1 style="font-size: 42px; margin: 0; color: #1e3a8a; font-weight: 900; letter-spacing: -1px;">IGLESIA ELIM</h1>
                    <p style="font-size: 18px; color: #3b82f6; font-weight: 800; margin: 10px 0; text-transform: uppercase;">
                        Estadísticas de Asistencia - ${sector}
                    </p>
                    <div style="font-size: 12px; color: #64748b; margin-top: 10px;">
                        Reporte generado por: <b>${this.state.user.name}</b> | Fecha: ${new Date().toLocaleDateString()}
                    </div>
                </div>

                <div style="display: table; width: 100%; margin-bottom: 30px; background: #f8fafc; padding: 25px; border-radius: 15px; border: 1px solid #e2e8f0; table-layout: fixed;">
                    <div style="display: table-cell; width: 50%; text-align: left; vertical-align: middle; padding-left: 20px;">
                        <span style="font-size: 11px; color: #64748b; font-weight: 800; text-transform: uppercase;">Discipulador</span>
                        <div style="font-size: 18px; font-weight: 800; color: #1e3a8a;">${leader}</div>
                    </div>
                    <div style="display: table-cell; width: 50%; text-align: right; vertical-align: middle; padding-right: 20px;">
                        <span style="font-size: 11px; color: #64748b; font-weight: 800; text-transform: uppercase;">Integrante</span>
                        <div style="font-size: 18px; font-weight: 800; color: #1e3a8a;">${sheep}</div>
                    </div>
                </div>

                <div style="display: table; width: 100%; margin-bottom: 40px; table-layout: fixed;">
                    <div style="display: table-cell; width: 50%; padding-right: 15px; text-align: center; background: #ffffff; border: 1px solid #f1f5f9; border-radius: 20px; padding: 20px;">
                        <div style="font-size: 11px; color: #64748b; margin-bottom: 15px; font-weight: 800; text-transform: uppercase;">Comparativa Semanal</div>
                        <img src="${chart1Img}" style="width: 100%; max-height: 300px;">
                    </div>
                    <div style="display: table-cell; width: 50%; padding-left: 15px; text-align: center; background: #ffffff; border: 1px solid #f1f5f9; border-radius: 20px; padding: 20px;">
                        <div style="font-size: 11px; color: #64748b; margin-bottom: 15px; font-weight: 800; text-transform: uppercase;">Tendencia General</div>
                        <img src="${chart2Img}" style="width: 100%; max-height: 300px;">
                    </div>
                </div>

                <div style="page-break-before: always; margin-bottom: 25px; border-left: 8px solid #2563eb; padding-left: 20px;">
                    <h2 style="font-size: 26px; margin: 0; color: #1e3a8a; font-weight: 900;">Tabla de Registro Detallado</h2>
                </div>

                <div class="pdf-table-container" style="width: 100%;">
                    <style>
                        table { width: 100%; border-collapse: collapse; border: 1px solid #cbd5e1; }
                        th { background: #f1f5f9; border: 1px solid #cbd5e1; padding: 12px 8px; font-size: 10px; color: #475569; text-transform: uppercase; font-weight: 900; }
                        td { border: 1px solid #cbd5e1; padding: 12px 8px; font-size: 11px; color: #1e293b; text-align: center; vertical-align: middle; }
                        td:first-child { text-align: left; font-weight: bold; width: 150px; background: #f8fafc; }
                        .pill { display: inline-block; width: 24px; height: 24px; border-radius: 50%; line-height: 24px; font-weight: 900; color: white; font-size: 9px; text-align: center; }
                        .pill-success { background: #10b981; }
                        .pill-danger { background: #ef4444; }
                        .sector-avatar { display: none; }
                        tr { page-break-inside: avoid !important; }
                    </style>
                    ${tableHtml}
                </div>

                <div style="margin-top: 60px; text-align: center; border-top: 2px solid #e2e8f0; padding-top: 30px;">
                    <b style="font-size: 12px; color: #1e3a8a;">IGLESIA ELIM - CONTROL PASTORAL</b>
                </div>
            </div>
        `;

        // 5. Opciones de PDF - CONFIGURACIÓN ANTI-CORTE
        const opt = {
            margin: [10, 10],
            filename: `Reporte_${sector}.pdf`,
            image: { type: 'jpeg', quality: 0.98 },
            html2canvas: {
                scale: 2,
                useCORS: true,
                logging: false,
                scrollY: 0,
                windowHeight: 5000 // Forzar altura gigante para capturar todo
            },
            jsPDF: { unit: 'mm', format: 'a4', orientation: 'portrait' },
            pagebreak: { mode: 'avoid-all' }
        };

        try {
            await html2pdf().set(opt).from(reportTemplate).save();
            this.notify('✅ Reporte generado correctamente');
        } catch (err) {
            console.error('Error:', err);
            this.notify('❌ Error al generar PDF', 'error');
        }
    }
};

// Export App to window for global access
window.App = App;

// Initialization is handled by index.html to ensure global-loader sync
// document.addEventListener('DOMContentLoaded', () => App.init());

// Namespace encapsulado para vista Historial de Pastor
window.ElimPastorUI = {
    state: {
        activeModule: 'all',
        activeType: 'all',
        activeId: null,
        activeParentId: null,
        activeYear: new Date().getFullYear(),
        activeModuleName: 'Todos los Módulos'
    },

    toggleFolder: function (el) {
        const children = el.nextElementSibling;
        if (children && children.classList.contains('folder-children')) {
            const isHidden = children.style.display === 'none';
            children.style.display = isHidden ? 'block' : 'none';
            const icon = el.querySelector('.folder-icon');
            if (icon) icon.innerText = isHidden ? '📂' : '📁';
            el.style.background = isHidden ? 'rgba(0,0,0,0.02)' : 'transparent';
        }
    },

    changeModule: function (btn, moduleId, year, moduleName) {
        document.querySelectorAll('.btn-module').forEach(el => {
            el.classList.remove('active');
            el.style.background = 'white';
            el.style.color = 'var(--text-muted)';
            el.style.borderColor = 'rgba(0,0,0,0.1)';
            el.style.boxShadow = 'none';
        });
        btn.classList.add('active');
        btn.style.background = 'var(--primary)';
        btn.style.color = 'white';
        btn.style.borderColor = 'transparent';
        btn.style.boxShadow = '0 4px 12px rgba(37,99,235,0.2)';

        this.state.activeModule = moduleId;
        if (year) this.state.activeYear = year;
        if (moduleName) this.state.activeModuleName = moduleName;

        this._applyFilters();
    },

    filterRender: function (type, id, parentSectorId) {
        // 1. Update active states in tree
        document.querySelectorAll('.folder-item').forEach(el => {
            el.classList.remove('active');
            el.style.background = 'transparent';
            if (el.classList.contains('sub-folder')) {
                el.style.color = 'var(--text-muted)';
            } else if (el.dataset.folderType === 'all') {
                el.style.color = 'var(--text-main)';
            } else {
                el.style.color = 'var(--text-main)';
            }
        });

        // Find clicked element
        const currentEvent = window.event;
        if (currentEvent) {
            let target = currentEvent.currentTarget;
            target.classList.add('active');
            if (target.classList.contains('sub-folder')) {
                target.style.background = 'rgba(37,99,235,0.05)';
                target.style.color = 'var(--primary)';
            } else {
                target.style.background = 'rgba(37,99,235,0.1)';
                target.style.color = 'var(--primary)';
            }
        }

        this.state.activeType = type;
        this.state.activeId = id;
        this.state.activeParentId = parentSectorId;

        this._applyFilters();
    },

    _applyFilters: function () {
        const cards = document.querySelectorAll('._pastor-card');
        let visibleCount = 0;
        const counts = { all: 0, sectors: {}, leaders: {} };

        cards.forEach(card => {
            const cSector = card.getAttribute('data-sector-id');
            const cLeader = card.getAttribute('data-leader-id');
            const cModule = card.getAttribute('data-module-id');

            // Re-tally for the active module ONLY
            if (this.state.activeModule === 'all' || this.state.activeModule === cModule) {
                counts.all++;
                counts.sectors[cSector] = (counts.sectors[cSector] || 0) + 1;
                counts.leaders[cLeader] = (counts.leaders[cLeader] || 0) + 1;
            }

            // Re-render visual list based on both module AND tree filters
            let showModule = (this.state.activeModule === 'all' || this.state.activeModule === cModule);
            let showTree = false;

            if (this.state.activeType === 'all') {
                showTree = true;
            } else if (this.state.activeType === 'sector') {
                showTree = (cSector === this.state.activeId);
            } else if (this.state.activeType === 'leader') {
                showTree = (cLeader === this.state.activeId);
            }

            let show = showModule && showTree;
            card.style.display = show ? 'block' : 'none';
            if (show) visibleCount++;
        });

        // Update tree counters directly
        const elAll = document.querySelector('.tree-count-all');
        if (elAll) elAll.innerText = counts.all;
        document.querySelectorAll('.tree-count-sector').forEach(el => {
            el.innerText = counts.sectors[el.getAttribute('data-id')] || 0;
        });
        document.querySelectorAll('.tree-count-leader').forEach(el => {
            el.innerText = counts.leaders[el.getAttribute('data-id')] || 0;
        });

        // Update Empty state
        const container = document.getElementById('pastorCardsContainer');
        let emptyMsg = document.getElementById('pastorEmptyMsg');

        if (visibleCount === 0) {
            if (!emptyMsg && container) {
                emptyMsg = document.createElement('div');
                emptyMsg.id = 'pastorEmptyMsg';
                emptyMsg.className = 'glass animate-reveal';
                emptyMsg.style = 'padding:40px; border-radius:24px; text-align:center; margin-top:20px; border:2px dashed rgba(0,0,0,0.05)';
                emptyMsg.innerHTML = '<div style="font-size:40px; margin-bottom:12px; opacity:0.5;">📂</div><h4 style="color:var(--text-main); font-size:16px;">Carpeta vacía</h4><p style="color:var(--text-muted); font-size:13px;">No hay reportes en esta selección</p>';
                container.appendChild(emptyMsg);
            }
            if (emptyMsg) emptyMsg.style.display = 'block';
        } else {
            if (emptyMsg) emptyMsg.style.display = 'none';
        }

        // 4. Update Breadcrumb
        const breadcrumb = document.getElementById('pastorBreadcrumb');
        if (breadcrumb) {
            let modText = this.state.activeModule === 'all' ? '' : `${this.state.activeModuleName} <span style="margin:0 8px; color:var(--text-muted); font-weight:400;">/</span> `;
            let treeText = '📂 Todos los Sectores';

            if (this.state.activeType === 'sector') {
                const target = document.querySelector(`.folder-item[data-folder-type="sector"][data-id="${this.state.activeId}"]`);
                if (target) treeText = `📁 ` + target.innerText.replace('📁', '').replace('📂', '').replace(/[\d]+$/, '').trim();
            } else if (this.state.activeType === 'leader') {
                const target = document.querySelector(`.folder-item.sub-folder[data-id="${this.state.activeId}"]`);
                const parent = document.querySelector(`.folder-item[data-folder-type="sector"][data-id="${this.state.activeParentId}"]`);
                let lName = target ? target.innerText.replace('↳', '').replace('👤', '').replace(/[\d]+$/, '').trim() : 'Discipulador';
                let sName = parent ? parent.innerText.replace('📁', '').replace('📂', '').replace(/[\d]+$/, '').trim() : 'Sector';
                treeText = `📁 ${sName} <span style="margin:0 8px; color:var(--text-muted); font-weight:400;">/</span> 👤 ${lName}`;
            }

            breadcrumb.innerHTML = `📅 Año ${this.state.activeYear} <span style="margin:0 8px; color:var(--text-muted); font-weight:400;">/</span> ${modText}${treeText} <span style="margin:0 8px; color:var(--text-muted); font-weight:400;">/</span> <span style="color:var(--text-muted); font-size:13px;"><span id="pastorVisibleCount">${visibleCount}</span> reportes</span>`;
        }
    }
};
