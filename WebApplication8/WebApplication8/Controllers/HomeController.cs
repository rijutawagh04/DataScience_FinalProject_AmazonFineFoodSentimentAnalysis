using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using WebApplication8.Models;

namespace WebApplication8.Controllers
{
    public class HomeController : Controller
    {
        public ActionResult Index()
        {
            return View();
        }

        public ActionResult About()
        {
            ViewBag.Message = "Amazon Food Reviews - Sentiment Analysis";

            return View();
        }

        public ActionResult Contact()
        {
            ViewBag.Message = "Need Assistance? Contact us at:";

            return View();
        }

        [HttpPost]
        public ActionResult GetData(Input input)
        {
            ReviewBLL review = new ReviewBLL();
            Output output = null;
            if (input.productID!=null && input.data!=null)
             output = review.getReviews(input.productID, input.data);
            if (output != null)
            {
                input.SL = output.SL.ToString();
                input.SP = output.SP.ToString();
            }
            return View(input);
        }
    }
}