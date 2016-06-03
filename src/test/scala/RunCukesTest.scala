import cucumber.api.CucumberOptions
import cucumber.api.junit.Cucumber
import org.junit.runner.RunWith

@RunWith(classOf[Cucumber])
@CucumberOptions(features = Array(
  "src/test/resources/features/fromS3ToRedshift.feature",
  "src/test/resources/features/readRedshift.feature",
  "src/test/resources/features/writeRedshift.feature"
))
class RunCukesTest