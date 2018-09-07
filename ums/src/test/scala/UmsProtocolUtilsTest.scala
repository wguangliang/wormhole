package scala

import edp.wormhole.ums.UmsFeedbackStatus
import edp.wormhole.ums.UmsProtocolUtils.feedbackDirective
import edp.wormhole.util.DateUtils

object UmsProtocolUtilsTest {
  def main(args:Array[String]) = {
    val directiveId = 66
    val streamId = 4

    val feedbackStatusSucess = feedbackDirective(DateUtils.currentDateTime, directiveId, UmsFeedbackStatus.SUCCESS, streamId, "")
    println(feedbackStatusSucess)

    val feedbackStatusFail = feedbackDirective(DateUtils.currentDateTime, directiveId, UmsFeedbackStatus.FAIL, streamId, "e.getMessage")
    println(feedbackStatusFail)


  }

}
