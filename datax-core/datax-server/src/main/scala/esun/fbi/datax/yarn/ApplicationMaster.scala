package esun.fbi.datax.yarn

import java.io.{File, PrintWriter}
import java.nio.charset.Charset
import java.util
import java.util.{UUID, Collections}

import akka.actor.Actor.Receive
import akka.actor.{ActorLogging, Actor, Props}
import akka.event.AddressTerminatedTopic
import com.typesafe.config.{ConfigFactory, Config}
import esun.fbi.datax.{Executor, Constants, DataxConf, JobScheduler}
import esun.fbi.datax.main.ThriftServerMain
import esun.fbi.datax.thrift.AkkaThriftServerHandler
import esun.fbi.datax.util.{Utils, AkkaUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{NMClient, AMRMClient}
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records
import org.apache.log4j.PropertyConfigurator

import scala.collection.JavaConversions._
object ApplicationMaster extends App {
  /**
   * 加载日志配置文件
   */
  PropertyConfigurator.configure("masterConf/log4j.properties")
  private val logging = org.slf4j.LoggerFactory.getLogger(classOf[ApplicationMaster])
  private val yarnConfiguration:Configuration = new YarnConfiguration()
  //yarnConfiguration.setInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,1000 * 60 * 60 * 2)//设置单个 application超时时间 未起作用

  private val dataxConf:DataxConf = new DataxConf()

  private val executorCores = dataxConf.getInt(Constants.DATAX_EXECUTOR_CORES,1)
  private var executorResource:Resource = Records.newRecord(classOf[Resource])
  private var executorPriority:Priority = Records.newRecord(classOf[Priority])



  executorResource = Records.newRecord(classOf[Resource])
  executorResource.setMemory(1636)
  executorResource.setVirtualCores(executorCores)

  executorPriority = Records.newRecord(classOf[Priority])
  executorPriority.setPriority(0)
  executorPriority = Records.newRecord(classOf[Priority])

  logging.info("create master actor system begin");
  val schedulerHost = dataxConf.getString(Constants.DATAX_MASTER_HOST,"127.0.0.1")
  val (schedulerSystem,port) = AkkaUtils.createActorSystem(Constants.AKKA_JOB_SCHEDULER_SYSTEM,schedulerHost,0,dataxConf)
  logging.info("create master actor system end");
  sys.addShutdownHook{
    schedulerSystem.shutdown()
  }
  private val jobSchedulerHostPost:String = s"${dataxConf.getString(Constants.DATAX_MASTER_HOST)}:$port"
  private val archiveName = Constants.DATAX_EXECUTOR_ARCHIVE_FILE_NAME
  private val executorCP = dataxConf.getString(
    Constants.DATAX_EXECUTOR_CP,
    s"$archiveName/*:$archiveName/lib/*:$archiveName/common/*:$archiveName/conf/*:$archiveName/datax/*:$archiveName/datax/lib/*:$archiveName/datax/common/*:$archiveName/datax/conf/*"
  )
  private val executorCmd:String = dataxConf.getString(
                Constants.DATAX_EXECUTOR_CMD,
                s"java -classpath $executorCP -Xms512M -Xmx1024M -XX:PermSize=128M -XX:MaxPermSize=512M esun.fbi.datax.Executor "
              ) +
              s" $jobSchedulerHostPost ${Constants.PLACEHOLDER_EXECUTOR_ID} ${Constants.EXECUTOR_RUN_ON_TYPE_YARN}" +
              s" 1> ${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stdout " +
              s" 2> ${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stderr"
  try {
    val hostPortWriter = new PrintWriter(new File("masterHostPort"),"UTF-8")
    hostPortWriter.print(jobSchedulerHostPost)
    hostPortWriter.close()
  }catch {
    case ex:Exception =>
      logging.error("writer host port error",ex)
    case _ => {}
  }

  logging.info("executor cmd:" + executorCmd)
  def getContainerCmd(container: Container) = {
    val executorId = Utils.containerIdNodeId2ExecutorId(container.getId,container.getNodeId)
    executorCmd.replace(Constants.PLACEHOLDER_EXECUTOR_ID,executorId)
  }
  private val nmClient:NMClient = NMClient.createNMClient()
  private val amrmClientAsync:AMRMClientAsync[AMRMClient.ContainerRequest] = AMRMClientAsync.createAMRMClientAsync(2000,new RMCallbackHandler(nmClient,getContainerCmd _,dataxConf,yarnConfiguration))

  amrmClientAsync.init(yarnConfiguration)

  amrmClientAsync.start()

  logging.info("register application master begin")
  amrmClientAsync.registerApplicationMaster("",0,"")
  logging.info("register application master end")
  nmClient.init(yarnConfiguration)
  nmClient.start();
  sys.addShutdownHook{
    amrmClientAsync.unregisterApplicationMaster(FinalApplicationStatus.UNDEFINED,"","")
  }

  val amActor = schedulerSystem.actorOf(Props(classOf[ApplicationMaster],dataxConf,yarnConfiguration,executorResource,executorPriority,amrmClientAsync,nmClient),Constants.AKKA_AM_ACTOR)

  val jobSchedulerActor = schedulerSystem.actorOf(Props(classOf[JobScheduler],dataxConf,amActor),Constants.AKKA_JOB_SCHEDULER_ACTOR)
  jobSchedulerActor ! "jobSchedulerActor started"

  logging.info(s"address:${jobSchedulerActor.path.address.hostPort}   ${jobSchedulerActor.path}  ${jobSchedulerActor.path.address}")

  logging.info(s"start thrift server begin")
  val thriftPort = dataxConf.getInt(Constants.THRIFT_SERVER_PORT,9777)
  val thriftHost = dataxConf.getString(Constants.THRIFT_SERVER_HOST,"127.0.0.1")
  val thriftConcurrence = dataxConf.getInt(Constants.THRIFT_SERVER_CONCURRENCE,8)
  val thriftServerHandler = new AkkaThriftServerHandler(jobSchedulerActor)

  logging.info(s"start thrift server on  $thriftHost:$thriftPort")
  try{
    ThriftServerMain.start(thriftConcurrence,thriftHost,thriftPort,thriftServerHandler)
  }catch {
    case ex:Exception =>
      amrmClientAsync.unregisterApplicationMaster(FinalApplicationStatus.UNDEFINED,"","")
      schedulerSystem.shutdown()
  }

}
/**
 * Created by zhuhq on 2016/4/27.
 */
class ApplicationMaster(
                         dataxConf: DataxConf,
                         yarnConfiguration: Configuration,
                         executorResource:Resource,
                         executorPriority:Priority,
                         aMRMClient: AMRMClientAsync[AMRMClient.ContainerRequest],
                         nMClient: NMClient
                         ) extends Actor with ActorLogging{


  val containerLocalCmd = dataxConf.getString(Constants.DATAX_EXECUTOR_LOCAL_CMD, "D:\\datax\\startExecutor.bat")
  def applyExecutor(count:Int = 1) = {
    applyExecutorYarn(count)
  }
  def applyExecutorLocal(count:Int = 1) = {
    val executorCount = math.max(count,1);
    log.info(s"apply executor local $count begin")
    for(_ <- 1 to executorCount) {
      val cmd =  containerLocalCmd + " " +
        ApplicationMaster.jobSchedulerHostPost + " " +
        UUID.randomUUID().toString + " " +
        Constants.EXECUTOR_RUN_ON_TYPE_LOCAL
      log.info(s"apply executor local cmd $cmd")
      sys.process.stringToProcess(cmd).run()
    }
    log.info(s"apply executor local $count end")
  }
  def applyExecutorYarn(count:Int = 1) = {
    val executorCount = math.max(count,1);
    log.info(s"apply executor yarn $count begin")
    for(_ <- 1 to executorCount) {
      val containerAsk = new ContainerRequest(executorResource,null,null,executorPriority);
      aMRMClient.addContainerRequest(containerAsk);
    }
    log.info(s"apply executor yarn $count end")

  }
  override def receive = {
    case ApplyExecutor(count) =>
      applyExecutor(count)
    case ApplyExecutorLocal(count) =>
      applyExecutorLocal(count)
    case ApplyExecutorYarn(count) =>
      applyExecutorYarn(count)
    case ReturnExecutor(executorId) =>
      Utils.executorId2ContainerIdNodeId(executorId) match {
        case Some((containerId,nodeId)) =>
          nMClient.stopContainer(containerId,nodeId)
          log.info(s"stop container of executor $executorId")
        case _ =>
          log.info(s"container of executor $executorId is not yarn container")
      }
    case _ => {

    }
  }

}
case class ApplyExecutor(count:Int = 1)
case class ApplyExecutorYarn(count:Int = 1)
case class ApplyExecutorLocal(count:Int = 1)
case class ReturnExecutor(executorId:String)
case class RegisterAM()
