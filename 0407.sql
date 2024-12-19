server:
  # Порты для прослушивания HTTP и gRPC запросов Promtail.
  http_listen_port: 9080
  grpc_listen_port: 0

positions:
  # Файл для хранения текущих позиций чтения файлов логов.
  filename: /tmp/positions.yaml

clients:
  # Адрес вашего Loki сервера, куда будут отправляться логи.
  - url: http://10.7.39.5:3100/loki/api/v1/push  # Укажите адрес вашего Loki сервера.

scrape_configs:
  - job_name: wildfly-logs
    static_configs:
      - targets:
          - localhost
        labels:
          host_ip: 10.7.39.15
          job: wildfly
          __path__: /u01/CM/wildfly/standalone/log/2_cm_errors.log  # Укажите путь к вашим логам WildFly.
    # Релабелинг для извлечения имени файла из полного пути.
    relabel_configs:
      - source_labels: ['__filename__']
        target_label: 'filename'
        regex: '.*/([^/]+)$'
        replacement: '$1'
    pipeline_stages:
      # Многострочная обработка логов.
      - multiline:
          # Регулярное выражение, определяющее начало новой записи лога.
          firstline: '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}'
          # Максимальное время ожидания следующей строки.
          max_wait_time: 3s
      # Разбор логов с помощью регулярного выражения.
      - regex:
          # Регулярное выражение для парсинга строк логов.
          expression: '^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) (?P<level>[A-Z]+) \[(?P<class>[^\]]+)\] \((?P<thread>[^)]+)\) (?P<message>.*)$'
      # Установка правильного временного штампа для каждой записи лога.
      - timestamp:
          source: timestamp
          format: "2006-01-02 15:04:05,000"
          location: Europe/Moscow
      # Преобразование извлеченных данных в метки и удаление ненужных меток.
      - labels:
          level:
          remove: "timestamp|thread|class"




Пример лога 
2024-12-20 02:00:38,927 WARN  [ru.intertrust.cm_sochi.srv.connector.sochi.notifications.pkd.SochiMailNotificationChannel] (EJB default - 1) not sended to personId = RdbmsId{typeId='5369', id=9211}, notificationType = EXEC1_52, reason = freemarker.template.TemplateModelException: Method "public ru.intertrust.cm.core.business.api.dto.IdentifiableObjectCollection ru.intertrust.cm.core.tools.Session.findByQuery(java.lang.String,java.lang.Object[])" threw an exception when invoked on ru.intertrust.cm.core.tools.Session object "ru.intertrust.cm.core.tools.Session@589bf345". See cause exception.

The failing instruction:
==> #assign collection = session.findByQu...  [in template "Template" at line 10, column 179]
2024-12-20 02:00:38,928 WARN  [ru.intertrust.cm_sochi.srv.connector.sochi.notifications.pkd.SochiMailNotificationChannel] (EJB default - 1) Notification for addressee RdbmsId{typeId='5022', id=9034} not send. Person email field is empty.
2024-12-20 02:00:38,928 WARN  [ru.intertrust.cm_sochi.srv.connector.sochi.notifications.pkd.SochiMailNotificationChannel] (EJB default - 1) not sended to personId = RdbmsId{typeId='5369', id=9210}, notificationType = EXEC1_52, reason = message not created
2024-12-20 02:00:39,010 ERROR [ru.intertrust.cm_sochi.srv.connector.sochi.notifications.pkd.SochiMailNotificationChannel] (EJB default - 5) Error send mail to null: ru.intertrust.cm.core.model.NotificationException: freemarker.template.TemplateModelException: Method "public ru.intertrust.cm.core.business.api.dto.IdentifiableObjectCollection ru.intertrust.cm.core.tools.Session.findByQuery(java.lang.String,java.lang.Object[])" threw an exception when invoked on ru.intertrust.cm.core.tools.Session object "ru.intertrust.cm.core.tools.Session@23a78b7". See cause exception.

The failing instruction:
==> #assign collection = session.findByQu...  [in template "Template" at line 10, column 179]
        at ru.intertrust.cm.core.business.impl.services.FreeMarkerFormatterImpl.format(FreeMarkerFormatterImpl.java:53)
        at ru.intertrust.cm.core.business.impl.services.FreeMarkerFormatterImpl.format(FreeMarkerFormatterImpl.java:34)
        at ru.intertrust.cm.core.business.impl.NotificationTextFormerImpl.formatTemplate(NotificationTextFormerImpl.java:140)
        at ru.intertrust.cm.core.business.impl.NotificationTextFormerImpl.format(NotificationTextFormerImpl.java:69)
        at ru.intertrust.cm_sochi.srv.connector.sochi.notifications.pkd.SochiMailNotificationChannel.createMailMessage(SochiMailNotificationChannel.java:430)
        at ru.intertrust.cm_sochi.srv.connector.sochi.notifications.pkd.SochiMailNotificationChannel.sendMailOneAddressee(SochiMailNotificationChannel.java:357)
        at ru.intertrust.cm_sochi.srv.connector.sochi.notifications.pkd.SochiMailNotificationChannel.sendMail(SochiMailNotificationChannel.java:235)
        at ru.intertrust.cm_sochi.srv.connector.sochi.notifications.pkd.SochiMailNotificationChannel.doSend(SochiMailNotificationChannel.java:184)
        at ru.intertrust.cm_sochi.srv.connector.sochi.notifications.pkd.SochiMailNotificationChannel.lambda$0(SochiMailNotificationChannel.java:161)
        at ru.intertrust.cm_sochi.srv.util.ctgtaskspool.CategorizedTasksPoolSync.addTask(CategorizedTasksPoolSync.java:42)
        at ru.intertrust.cm_sochi.srv.connector.sochi.notifications.pkd.PkdBaseChannel.send(PkdBaseChannel.java:93)
        at ru.intertrust.cm_sochi.srv.connector.sochi.notifications.pkd.SochiMailNotificationChannel.send(SochiMailNotificationChannel.java:154)
        at ru.intertrust.cm.core.business.impl.NotificationServiceImpl.sendSync(NotificationServiceImpl.java:247)
        at ru.intertrust.cm.core.business.impl.NotificationServiceImpl.sendNow(NotificationServiceImpl.java:98)
        at sun.reflect.GeneratedMethodAccessor1112.invoke(Unknown Source)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:498)
        at org.jboss.as.ee.component.ManagedReferenceMethodInterceptor.processInvocation(ManagedReferenceMethodInterceptor.java:52)
        at org.jboss.invocation.InterceptorContext.proceed(InterceptorContext.java:422)
        at org.jboss.invocation.InterceptorContext$Invocation.proceed(InterceptorContext.java:509)
        at org.jboss.as.weld.interceptors.Jsr299BindingsInterceptor.delegateInterception(Jsr299BindingsInterceptor.java:79)
        at org.jboss.as.weld.interceptors.Jsr299BindingsInterceptor.doMethodInterception(Jsr299BindingsInterceptor.java:89)
        at org.jboss.as.weld.interceptors.Jsr299BindingsInterceptor.processInvocation(Jsr299BindingsInterceptor.java:102)
        at org.jboss.as.ee.component.interceptors.UserInterceptorFactory$1.processInvocation(UserInterceptorFactory.java:63)
        at org.jboss.invocation.InterceptorContext.proceed(InterceptorContext.java:422)
        at org.jboss.as.ejb3.component.invocationmetrics.ExecutionTimeInterceptor.processInvocation(ExecutionTimeInterceptor.java:43)
        at org.jboss.invocation.InterceptorContext.proceed(InterceptorContext.java:422)
        at org.jboss.as.jpa.interceptor.SBInvocationInterceptor.processInvocation(SBInvocationInterceptor.java:47)
        at org.jboss.invocation.InterceptorContext.proceed(InterceptorContext.java:422)
        at org.jboss.as.ee.concurrent.ConcurrentContextInterceptor.processInvocation(ConcurrentContextInterceptor.java:45)
        at org.jboss.invocation.InterceptorContext.proceed(InterceptorContext.java:422)
        at org.jboss.invocation.InitialInterceptor.processInvocation(InitialInterceptor.java:40)
        at org.jboss.invocation.InterceptorContext.proceed(InterceptorContext.java:422)
        at org.jboss.invocation.ChainedInterceptor.processInvocation(ChainedInterceptor.java:53)
        at org.jboss.as.ee.component.interceptors.ComponentDispatcherInterceptor.processInvocation(ComponentDispatcherInterceptor.java:52)
        at org.jboss.invocation.InterceptorContext.proceed(InterceptorContext.java:422)
        at org.jboss.as.ejb3.component.pool.PooledInstanceInterceptor.processInvocation(PooledInstanceInterceptor.java:51)
        at org.jboss.invocation.InterceptorContext.proceed(InterceptorContext.java:422)
        at org.jboss.as.ejb3.component.interceptors.AdditionalSetupInterceptor.processInvocation(AdditionalSetupInterceptor.java:54)
        at org.jboss.invocation.InterceptorContext.proceed(InterceptorContext.java:422)
        at org.jboss.as.ejb3.tx.CMTTxInterceptor.invokeInOurTx(CMTTxInterceptor.java:237)
        at org.jboss.as.ejb3.tx.CMTTxInterceptor.required(CMTTxInterceptor.java:362)
        at org.jboss.as.ejb3.tx.CMTTxInterceptor.processInvocation(CMTTxInterceptor.java:144)
        at org.jboss.invocation.InterceptorContext.proceed(InterceptorContext.java:422)
        at org.jboss.invocation.InterceptorContext$Invocation.proceed(InterceptorContext.java:509)
        at org.jboss.weld.module.ejb.AbstractEJBRequestScopeActivationInterceptor.aroundInvoke(AbstractEJBRequestScopeActivationInterceptor.java:81)
        at org.jboss.as.weld.ejb.EjbRequestScopeActivationInterceptor.processInvocation(EjbRequestScopeActivationInterceptor.java:89)
        at org.jboss.invocation.InterceptorContext.proceed(InterceptorContext.java:422)
        at org.jboss.as.ejb3.component.interceptors.CurrentInvocationContextInterceptor.processInvocation(CurrentInvocationContextInterceptor.java:41)
        at org.jboss.invocation.InterceptorContext.proceed(InterceptorContext.java:422)
        at org.jboss.as.ejb3.component.invocationmetrics.WaitTimeInterceptor.processInvocation(WaitTimeInterceptor.java:47)
        at org.jboss.invocation.InterceptorContext.proceed(InterceptorContext.java:422)
        at org.jboss.as.ejb3.security.SecurityContextInterceptor.processInvocation(SecurityContextInterceptor.java:100)
        at org.jboss.invocation.InterceptorContext.proceed(InterceptorContext.java:422)
        at org.jboss.as.ejb3.deployment.processors.StartupAwaitInterceptor.processInvocation(StartupAwaitInterceptor.java:22)
        at org.jboss.invocation.InterceptorContext.proceed(InterceptorContext.java:422)
        at org.jboss.as.ejb3.component.interceptors.ShutDownInterceptorFactory$1.processInvocation(ShutDownInterceptorFactory.java:64)
        at org.jboss.invocation.InterceptorContext.proceed(InterceptorContext.java:422)
        at org.jboss.as.ejb3.component.interceptors.LoggingInterceptor.processInvocation(LoggingInterceptor.java:67)
        at org.jboss.invocation.InterceptorContext.proceed(InterceptorContext.java:422)
        at org.jboss.as.ee.component.NamespaceContextInterceptor.processInvocation(NamespaceContextInterceptor.java:50)
        at org.jboss.invocation.InterceptorContext.proceed(InterceptorContext.java:422)
        at org.jboss.invocation.ContextClassLoaderInterceptor.processInvocation(ContextClassLoaderInterceptor.java:60)
        at org.jboss.invocation.InterceptorContext.proceed(InterceptorContext.java:422)
        at org.jboss.invocation.InterceptorContext.run(InterceptorContext.java:438)
        at org.wildfly.security.manager.WildFlySecurityManager.doChecked(WildFlySecurityManager.java:627)
        at org.jboss.invocation.AccessCheckingInterceptor.processInvocation(AccessCheckingInterceptor.java:57)
        at org.jboss.invocation.InterceptorContext.proceed(InterceptorContext.java:422)
        at org.jboss.invocation.ChainedInterceptor.processInvocation(ChainedInterceptor.java:53)
        at org.jboss.as.ee.component.ViewService$View.invoke(ViewService.java:198)
        at org.jboss.as.ee.component.ViewDescription$1.processInvocation(ViewDescription.java:185)
        at org.jboss.invocation.InterceptorContext.proceed(InterceptorContext.java:422)
        at org.jboss.as.ejb3.component.interceptors.LogDiagnosticContextRecoveryInterceptor.processInvocation(LogDiagnosticContextRecoveryInterceptor.java:82)
        at org.jboss.invocation.InterceptorContext.proceed(InterceptorContext.java:422)
        at org.jboss.as.ejb3.component.interceptors.AsyncFutureInterceptorFactory$2$2.runInvocation(AsyncFutureInterceptorFactory.java:152)
        at org.jboss.as.ejb3.component.interceptors.AsyncInvocationTask.run(AsyncInvocationTask.java:81)
        at org.jboss.threads.ContextClassLoaderSavingRunnable.run(ContextClassLoaderSavingRunnable.java:35)
        at org.jboss.threads.EnhancedQueueExecutor.safeRun(EnhancedQueueExecutor.java:1982)
        at org.jboss.threads.EnhancedQueueExecutor$ThreadBody.doRunTask(EnhancedQueueExecutor.java:1486)
        at org.jboss.threads.EnhancedQueueExecutor$ThreadBody.run(EnhancedQueueExecutor.java:1377)
        at java.lang.Thread.run(Thread.java:750)
        at org.jboss.threads.JBossThread.run(JBossThread.java:485)
Caused by: freemarker.template.TemplateModelException: Method "public ru.intertrust.cm.core.business.api.dto.IdentifiableObjectCollection ru.intertrust.cm.core.tools.Session.findByQuery(java.lang.String,java.lang.Object[])" threw an exception when invoked on ru.intertrust.cm.core.tools.Session object "ru.intertrust.cm.core.tools.Session@23a78b7". See cause exception.

The failing instruction:
==> #assign collection = session.findByQu...  [in template "Template" at line 10, column 179]
        at freemarker.ext.beans.SimpleMethodModel.exec(SimpleMethodModel.java:135)
        at freemarker.core.MethodCall._eval(MethodCall.java:98)
        at freemarker.core.Expression.eval(Expression.java:111)
        at freemarker.core.Assignment.accept(Assignment.java:106)
        at freemarker.core.Environment.visit(Environment.java:265)
        at freemarker.core.MixedContent.accept(MixedContent.java:93)
        at freemarker.core.Environment.visit(Environment.java:265)
        at freemarker.core.Environment.process(Environment.java:243)
        at freemarker.template.Template.process(Template.java:277)
        at ru.intertrust.cm.core.business.impl.services.FreeMarkerFormatterImpl.format(FreeMarkerFormatterImpl.java:50)
        ... 81 more
Caused by: java.lang.NullPointerException
        at ru.intertrust.cm.core.tools.Session.findByQuery(Session.java:70)
        at sun.reflect.GeneratedMethodAccessor1091.invoke(Unknown Source)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:498)
        at freemarker.ext.beans.BeansWrapper.invokeMethod(BeansWrapper.java:912)
        at freemarker.ext.beans.SimpleMethodModel.exec(SimpleMethodModel.java:107)
        ... 90 more

2024-12-20 02:00:39,010 WARN  [ru.intertrust.cm_sochi.srv.connector.sochi.notifications.pkd.SochiMailNotificationChannel] (EJB default - 5) not sended to personId = RdbmsId{typeId='5369', id=7788}, notificationType = EXEC1_52, reason = freemarker.template.TemplateModelException: Method "public ru.intertrust.cm.core.business.api.dto.IdentifiableObjectCollection ru.intertrust.cm.core.tools.Session.findByQuery(java.lang.String,java.lang.Object[])" threw an exception when invoked on ru.intertrust.cm.core.tools.Session object "ru.intertrust.cm.core.tools.Session@23a78b7". See cause exception.
