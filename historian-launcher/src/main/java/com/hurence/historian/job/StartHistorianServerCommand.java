package com.hurence.historian.job;


import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.hurence.webapiservice.WebApiServiceMainVerticle;
import com.hurence.webapiservice.http.HttpServerVerticle;
import com.hurence.webapiservice.http.HttpVerticleConf;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.file.FileSystem;
import io.vertx.reactivex.ext.web.client.WebClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;

import static com.hurence.historian.job.Properties.HISTORIAN_SERVER_JAR;
import static com.hurence.historian.job.Properties.PIDS_DIR;

@Command(name = "start-historian-server", description = "Start an instance of the historian server")
public class StartHistorianServerCommand {

    private static Logger LOGGER = LoggerFactory.getLogger(StartHistorianServerCommand.class);

    @Option(name = { "-l", "--log-conf" }, description = "if we want TRACE level log or not (should not be used in production environment).")
    private String logConfFilePath;

    @Option(name = { "-c", "--conf" }, description = "The path to the conf file for historian server.")
    private String confFilePath;

    private final Vertx vertx = Vertx.vertx();
    private final FileSystem fs = vertx.fileSystem();
    private HttpVerticleConf conf;

    public static void main(String[] args) {
        SingleCommand<StartHistorianServerCommand> parser = SingleCommand.singleCommand(StartHistorianServerCommand.class);
        StartHistorianServerCommand cmd = parser.parse(args);
        cmd.run();
    }
//    ROOT_DIR == ../ par rapport a bin/
//    "-Dlog4j.configuration=file:$ROOT_DIR/conf/log4j-debug.properties"
//    "-Dlog4j.configuration=file:$ROOT_DIR/conf/log4j.properties"
    private void run() {
        LOGGER.info("will start historian-server with conf file : {}", confFilePath);
        try {
            conf = parseHttpConf();
            String pidFilePath = PIDS_DIR + File.separator + String.format("app-%s.pid", conf.getHttpPort());
            checkIfIsAlreadyRunning(pidFilePath, conf.getHttpPort());
            startHistorianAndWritePid(pidFilePath);
            try {
                WaitUntilHistorianIsReadyOrFail();
                LOGGER.info("Successfully Started historian with hostname {} on port {}", conf.getHostName(), conf.getHttpPort());
            } catch (Exception ex) {
                LOGGER.error("Could not join the historian. Something seems to have gone wrong");
                fs.deleteBlocking(pidFilePath);
            }
        } finally {
            vertx.close();
        }
    }

    private void WaitUntilHistorianIsReadyOrFail() {
        WebClient webClient = WebClient.create(vertx, new WebClientOptions()
                .setDefaultHost(conf.getHostName())
                .setDefaultPort(conf.getHttpPort()));
        webClient
                .get(HttpServerVerticle.MAIN_API_ENDPOINT)
                .rxSend()
                .doOnSuccess()
            ;
        //TODO
    }

    private void startHistorianAndWritePid(String pidFilePath) {
        fs.createFileBlocking(pidFilePath);
        try {
            long pid = runProcessThenGetPid();
            fs.writeFileBlocking(pidFilePath, Buffer.buffer().appendLong(pid));
        } catch (IOException e) {
            LOGGER.error("An error occured while starting historian", e);
            fs.deleteBlocking(pidFilePath);
            System.exit(0);
        }
    }

    private long runProcessThenGetPid() throws IOException {
        ProcessBuilder p = new ProcessBuilder("java",
                "-Dlog4j.configuration=file:" + logConfFilePath,
                "-jar", HISTORIAN_SERVER_JAR,
                "--conf", confFilePath);
        LOGGER.info("starting historian with hostname {} on port {}", conf.getHostName(), conf.getHttpPort());
        Process process = p.start();
        return getPidOfProcess(process);
    }

    public static synchronized long getPidOfProcess(Process p) {
        try {
            if (p.getClass().getName().equals("java.lang.UNIXProcess")) {
                Field f = p.getClass().getDeclaredField("pid");
                f.setAccessible(true);
                long pid = f.getLong(p);
                f.setAccessible(false);
                return pid;
            } else {
                throw new IllegalArgumentException("Can not find pid of process on this OS");
            }
        } catch (Exception e) {
            LOGGER.error("An error happening while trying to get the pid of the process.");
            throw new RuntimeException(e);
        }
    }

    private boolean checkIfIsAlreadyRunning(String pidFilePath, int port) {
        if (fs.existsBlocking(pidFilePath)) {
            LOGGER.error("historian seems to be already running on port {}", port);
            System.exit(1);
        }
    }

    private HttpVerticleConf parseHttpConf() {
        try {
            JsonObject jsonConfFile = fs.readFileBlocking(confFilePath).toJsonObject();
            return new HttpVerticleConf(WebApiServiceMainVerticle.extractHttpConf(jsonConfFile));
        } catch (Exception ex) {
            LOGGER.error("Failed to parse conf file as a json object", ex);
            System.exit(1);
        }
    }
}

