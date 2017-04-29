using Microsoft.Owin;
using Owin;

[assembly: OwinStartupAttribute(typeof(WebApplication8.Startup))]
namespace WebApplication8
{
    public partial class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            ConfigureAuth(app);
        }
    }
}
